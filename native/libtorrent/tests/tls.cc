#include <openssl/err.h>
#include <openssl/ssl.h>
#include <array>
#include <iostream>
#include <memory>
#include <stdexcept>

static void require(bool value, char const* message) {
    if (!value) throw std::runtime_error(message);
}

// Exercise libssl itself, including the TLS methods whose object files formerly
// also retained QUIC. Memory BIOs avoid network and scheduler dependencies.
static void handshake(int version, bool trusted) {
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
        EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"), EVP_PKEY_free);
    std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), X509_free);
    require(key && cert, "TLS identity allocation");
    require(X509_set_version(cert.get(), 2) == 1
        && ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1) == 1
        && X509_gmtime_adj(X509_getm_notBefore(cert.get()), -60)
        && X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600)
        && X509_set_pubkey(cert.get(), key.get()) == 1, "TLS identity fields");
    auto* name = X509_get_subject_name(cert.get());
    require(X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
        reinterpret_cast<unsigned char const*>("localhost"), -1, -1, 0) == 1
        && X509_set_issuer_name(cert.get(), name) == 1
        && X509_sign(cert.get(), key.get(), EVP_sha256()) > 0, "TLS identity signing");
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> client_ctx(
        SSL_CTX_new(TLS_client_method()), SSL_CTX_free);
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> server_ctx(
        SSL_CTX_new(TLS_server_method()), SSL_CTX_free);
    require(client_ctx && server_ctx, "TLS context allocation");
    for (auto* ctx : {client_ctx.get(), server_ctx.get()}) {
        require(SSL_CTX_set_min_proto_version(ctx, version) == 1
            && SSL_CTX_set_max_proto_version(ctx, version) == 1, "TLS version bounds");
    }
    require(SSL_CTX_use_certificate(server_ctx.get(), cert.get()) == 1
        && SSL_CTX_use_PrivateKey(server_ctx.get(), key.get()) == 1, "TLS server identity");
    SSL_CTX_set_verify(client_ctx.get(), SSL_VERIFY_PEER, nullptr);
    if (trusted) {
        require(X509_STORE_add_cert(SSL_CTX_get_cert_store(client_ctx.get()), cert.get()) == 1,
            "TLS trust anchor");
    }
    std::unique_ptr<SSL, decltype(&SSL_free)> client(SSL_new(client_ctx.get()), SSL_free);
    std::unique_ptr<SSL, decltype(&SSL_free)> server(SSL_new(server_ctx.get()), SSL_free);
    require(client && server, "TLS connection allocation");
    require(SSL_set1_host(client.get(), "localhost") == 1, "TLS peer name");
    BIO* client_bio = nullptr;
    BIO* server_bio = nullptr;
    require(BIO_new_bio_pair(&client_bio, 0, &server_bio, 0) == 1, "TLS BIO pair");
    // Each SSL takes ownership of its single shared read/write BIO.
    SSL_set_bio(client.get(), client_bio, client_bio);
    SSL_set_bio(server.get(), server_bio, server_bio);
    SSL_set_connect_state(client.get());
    SSL_set_accept_state(server.get());
    bool failed = false;
    for (int attempt = 0; attempt < 100; ++attempt) {
        for (auto* ssl : {client.get(), server.get()}) {
            if (SSL_is_init_finished(ssl)) continue;
            ERR_clear_error();
            int const result = SSL_do_handshake(ssl);
            if (result != 1) {
                int const error = SSL_get_error(ssl, result);
                if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) {
                    failed = true;
                    break;
                }
            }
        }
        if (failed || (SSL_is_init_finished(client.get()) && SSL_is_init_finished(server.get()))) break;
    }
    if (!trusted) {
        require(failed && SSL_get_verify_result(client.get()) != X509_V_OK,
            "TLS untrusted certificate must fail verification");
        ERR_clear_error();
        return;
    }
    require(!failed && SSL_is_init_finished(client.get()) && SSL_is_init_finished(server.get())
        && SSL_version(client.get()) == version, "TLS handshake completion");
    std::array<unsigned char, 5> const message{{'a', 'r', 'i', 'a', 'x'}};
    std::array<unsigned char, 5> received{};
    std::size_t size = 0;
    require(SSL_write_ex(client.get(), message.data(), message.size(), &size) == 1
        && size == message.size(), "TLS application write");
    require(SSL_read_ex(server.get(), received.data(), received.size(), &size) == 1
        && size == received.size() && received == message, "TLS application read");
}

int main() try {
    for (int version : {TLS1_2_VERSION, TLS1_3_VERSION}) {
        handshake(version, true);
        handshake(version, false);
    }
    std::cout << "Native TLS 1.2/1.3 trust and payload checks passed.\n";
} catch (std::exception const& error) {
    std::cerr << error.what() << '\n';
    ERR_print_errors_fp(stderr);
    return 1;
}
