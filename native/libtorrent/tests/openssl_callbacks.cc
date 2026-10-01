#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/safestack.h>
#include <openssl/x509.h>
#include <array>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

static void require(bool value, char const* message) {
    if (!value) throw std::runtime_error(message);
}

struct Item { int value; };
DEFINE_STACK_OF(Item)
static int copies_remaining = -1;
static int freed = 0;
static int compare_item(Item const* const* a, Item const* const* b) {
    return ((*a)->value > (*b)->value) - ((*a)->value < (*b)->value);
}
static Item* copy_item(Item const* item) {
    if (copies_remaining == 0) return nullptr;
    if (copies_remaining > 0) --copies_remaining;
    return new Item(*item);
}
static void free_item(Item* item) { ++freed; delete item; }
static void free_items(STACK_OF(Item)* stack) { sk_Item_pop_free(stack, free_item); }
using Items = std::unique_ptr<STACK_OF(Item), decltype(&free_items)>;

static void typed_stacks() {
    for (int constructor = 0; constructor < 3; ++constructor) {
        Items items(constructor == 0 ? sk_Item_new(compare_item)
            : constructor == 1 ? sk_Item_new_null() : sk_Item_new_reserve(compare_item, 4), free_items);
        require(bool(items), "typed stack allocation");
        sk_Item_set_cmp_func(items.get(), compare_item);
        Item key{2};
        require(sk_Item_find(items.get(), &key) == -1, "empty stack lookup");
        for (int value : {3, 1, 2, 2}) {
            auto item = std::make_unique<Item>(Item{value});
            require(sk_Item_push(items.get(), item.get()) > 0, "typed stack push");
            item.release();
        }
        require(sk_Item_find(items.get(), &key) == 2, "unsorted lookup");
        sk_Item_sort(items.get());
        require(sk_Item_is_sorted(items.get()), "explicit sort");
        int matches = 0;
        require(sk_Item_find_all(items.get(), &key, &matches) == 1 && matches == 2,
            "sorted lookup includes both equal items");
        key.value = 9;
        require(sk_Item_find(items.get(), &key) == -1, "missing item rejected");
        copies_remaining = -1;
        Items copy(sk_Item_deep_copy(items.get(), copy_item, free_item), free_items);
        require(copy && sk_Item_num(copy.get()) == 4, "deep copy");
        require(sk_Item_value(copy.get(), 0) != sk_Item_value(items.get(), 0)
            && sk_Item_value(copy.get(), 0)->value == 1, "independent copied values");
        auto const baseline = freed;
        copies_remaining = 1;
        Items failed(sk_Item_deep_copy(items.get(), copy_item, free_item), free_items);
        require(!failed && freed == baseline + 1, "failed copy frees its completed item");
        require(sk_Item_num(items.get()) == 4, "failed copy preserves source");
    }
    // A typed duplicate of an empty generic stack must install its thunks too.
    Items empty(sk_Item_dup(nullptr), free_items);
    require(bool(empty), "null stack duplication");
    auto item = std::make_unique<Item>(Item{7});
    require(sk_Item_push(empty.get(), item.get()) == 1, "push after null duplication");
    item.release();
    copies_remaining = -1;
    Items copy(sk_Item_deep_copy(empty.get(), copy_item, free_item), free_items);
    require(copy && sk_Item_value(copy.get(), 0)->value == 7, "copy after null duplication");
}

static std::vector<unsigned char> bytes(std::string const& hex) {
    std::vector<unsigned char> result;
    for (std::size_t i = 0; i < hex.size(); i += 2)
        result.push_back(static_cast<unsigned char>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    return result;
}

static void random_and_digests() {
    std::array<unsigned char, 32> random{};
    require(RAND_bytes(random.data(), int(random.size())) == 1, "provider random generation");
    struct Vector { EVP_MD const* (*algorithm)(); char const* expected; };
    for (auto const& vector : {
            Vector{EVP_sha1, "a9993e364706816aba3e25717850c26c9cd0d89d"},
            Vector{EVP_sha224, "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"},
            Vector{EVP_sha256, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
            Vector{EVP_sha384, "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"},
            Vector{EVP_sha512, "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"}}) {
        std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
        unsigned size = 0;
        require(EVP_Digest("abc", 3, digest.data(), &size, vector.algorithm(), nullptr) == 1,
            "provider digest");
        require(std::vector<unsigned char>(digest.begin(), digest.begin() + size) == bytes(vector.expected),
            "known digest vector");
    }
    auto* missing = EVP_MD_fetch(nullptr, "ariax-nonexistent-digest", nullptr);
    require(missing == nullptr, "unknown digest rejected");
    ERR_clear_error();
}

static void aes_vectors() {
    // NIST SP 800-38A, F.1.1 (ECB) and F.5.1 (CTR), first block.
    auto const key = bytes("2b7e151628aed2a6abf7158809cf4f3c");
    auto const iv = bytes("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
    auto const plain = bytes("6bc1bee22e409f96e93d7e117393172a");
    for (bool ctr : {false, true}) {
        auto const cipher = bytes(ctr ? "874d6191b620e3261bef6864990db6ce"
                                      : "3ad77bb40d7a3660a89ecaf32466ef97");
        for (int encrypt : {0, 1}) {
            std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>
                ctx(EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free);
            require(bool(ctx), "cipher allocation");
            require(EVP_CipherInit_ex(ctx.get(), ctr ? EVP_aes_128_ctr() : EVP_aes_128_ecb(),
                nullptr, key.data(), iv.data(), encrypt) == 1, "cipher initialization");
            require(EVP_CIPHER_CTX_set_key_length(ctx.get(), 15) == 0, "invalid AES key length rejected");
            ERR_clear_error();
            require(EVP_CIPHER_CTX_set_padding(ctx.get(), 0) == 1, "disable ECB padding");
            auto const& input = encrypt ? plain : cipher;
            std::array<unsigned char, 32> output{};
            int size = 0, final_size = 0;
            require(EVP_CipherUpdate(ctx.get(), output.data(), &size, input.data(), int(input.size())) == 1,
                "cipher update");
            require(EVP_CipherFinal_ex(ctx.get(), output.data() + size, &final_size) == 1, "cipher final");
            require(std::vector<unsigned char>(output.begin(), output.begin() + size + final_size)
                == (encrypt ? cipher : plain), "known AES vector");
        }
    }
}

static void certificates() {
    std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file(ARIAX_OPENSSL_CERT, "rb"), BIO_free);
    require(bool(input), "certificate fixture");
    auto free_infos = [](STACK_OF(X509_INFO)* infos) { sk_X509_INFO_pop_free(infos, X509_INFO_free); };
    std::unique_ptr<STACK_OF(X509_INFO), decltype(free_infos)>
        infos(PEM_X509_INFO_read_bio(input.get(), nullptr, nullptr, nullptr), free_infos);
    require(infos && sk_X509_INFO_num(infos.get()) == 1, "PEM certificate decoder");
    auto* cert = sk_X509_INFO_value(infos.get(), 0)->x509;
    require(cert != nullptr, "decoded certificate");
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(X509_get_pubkey(cert), EVP_PKEY_free);
    require(key && EVP_PKEY_is_a(key.get(), "RSA"), "RSA public key decoder");
    std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)> store(X509_STORE_new(), X509_STORE_free);
    require(store && X509_STORE_add_cert(store.get(), cert) == 1
        && X509_STORE_add_cert(store.get(), cert) == 1, "duplicate certificate store lookup");
    unsigned char* encoded = nullptr;
    int const size = i2d_X509(cert, &encoded);
    std::unique_ptr<unsigned char, void (*)(unsigned char*)> der(encoded,
        [](unsigned char* data) { OPENSSL_free(data); });
    require(size > 0 && der, "const ASN.1 callback encoding");
    auto const* cursor = der.get();
    std::unique_ptr<X509, decltype(&X509_free)> decoded(d2i_X509(nullptr, &cursor, size), X509_free);
    require(decoded && cursor == der.get() + size, "DER round trip");
    unsigned char const invalid[] = {0x30, 0x01, 0xff};
    cursor = invalid;
    std::unique_ptr<X509, decltype(&X509_free)> rejected(d2i_X509(nullptr, &cursor, sizeof(invalid)), X509_free);
    require(!rejected, "malformed certificate rejected");
    ERR_clear_error();
}

int main() try {
    struct Stage { char const* name; void (*run)(); };
    for (auto const& stage : {Stage{"typed stacks", typed_stacks},
            Stage{"random and digests", random_and_digests},
            Stage{"AES vectors", aes_vectors}, Stage{"certificate callbacks", certificates}}) {
        std::cerr << "OpenSSL: " << stage.name << std::endl;
        stage.run();
    }
    std::cout << "OpenSSL callback regression passed." << std::endl;
} catch (std::exception const& error) {
    std::cerr << error.what() << '\n';
    ERR_print_errors_fp(stderr);
    return 1;
}
