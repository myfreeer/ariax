//! Optional RSA backend for SSH authentication and exchange signatures.
//! Key serialization remains owned by ssh-key. Never retry with RustCrypto.

use openssl::bn::BigNum;
use openssl::hash::MessageDigest;
use openssl::pkey::PKey;
use openssl::rsa::{Padding, Rsa, RsaPrivateKeyBuilder};
use openssl::sign::{Signer, Verifier};
use ssh_key::{Algorithm, Error, HashAlg, Mpint, Signature};

fn digest(hash: Option<HashAlg>) -> ssh_key::Result<MessageDigest> {
    match hash {
        Some(HashAlg::Sha256) => Ok(MessageDigest::sha256()),
        Some(HashAlg::Sha512) => Ok(MessageDigest::sha512()),
        _ => Err(Error::Crypto),
    }
}

fn number(value: &Mpint) -> ssh_key::Result<BigNum> {
    // Secure BIGNUM storage is cleared on release, including conversion errors.
    let mut number = BigNum::new_secure().map_err(|_| Error::Crypto)?;
    number
        .copy_from_slice(value.as_positive_bytes().ok_or(Error::Crypto)?)
        .map_err(|_| Error::Crypto)?;
    Ok(number)
}

fn check_public(key: &ssh_key::public::RsaPublicKey) -> ssh_key::Result<()> {
    // Preserve ssh-key's existing modulus/exponent admission limits before
    // entering OpenSSL. This performs public validation, not an RSA operation.
    rsa::RsaPublicKey::try_from(key).map(|_| ())
}

pub(crate) fn sign(
    key: &ssh_key::private::RsaKeypair,
    hash: Option<HashAlg>,
    data: &[u8],
) -> ssh_key::Result<Signature> {
    let digest = digest(hash)?;
    let public = key.public();
    check_public(public)?;
    let private = key.private();
    // Import components directly. In particular, do not construct a RustCrypto
    // SigningKey or compute secret CRT values in Rust. OpenSSL supports d/p/q
    // without cached CRT exponents and retains its default RSA blinding.
    let rsa = RsaPrivateKeyBuilder::new(
        number(public.n())?,
        number(public.e())?,
        number(private.d())?,
    )
    .map_err(|_| Error::Crypto)?
    .set_factors(number(private.p())?, number(private.q())?)
    .map_err(|_| Error::Crypto)?
    .build();
    if !rsa.check_key().map_err(|_| Error::Crypto)? {
        return Err(Error::Crypto);
    }
    let key = PKey::from_rsa(rsa).map_err(|_| Error::Crypto)?;
    let mut signer = Signer::new(digest, &key).map_err(|_| Error::Crypto)?;
    signer
        .set_rsa_padding(Padding::PKCS1)
        .map_err(|_| Error::Crypto)?;
    let signature = signer
        .sign_oneshot_to_vec(data)
        .map_err(|_| Error::Crypto)?;
    Signature::new(Algorithm::Rsa { hash }, signature)
}

pub(crate) fn verify(
    key: &ssh_key::public::RsaPublicKey,
    data: &[u8],
    signature: &Signature,
) -> ssh_key::Result<()> {
    let Algorithm::Rsa { hash } = signature.algorithm() else {
        return Err(Error::Crypto);
    };
    let digest = digest(hash)?;
    check_public(key)?;
    let rsa = Rsa::from_public_components(number(key.n())?, number(key.e())?)
        .map_err(|_| Error::Crypto)?;
    if signature.as_bytes().len() != rsa.size() as usize {
        return Err(Error::Crypto);
    }
    let key = PKey::from_rsa(rsa).map_err(|_| Error::Crypto)?;
    let mut verifier = Verifier::new(digest, &key).map_err(|_| Error::Crypto)?;
    verifier
        .set_rsa_padding(Padding::PKCS1)
        .map_err(|_| Error::Crypto)?;
    if verifier
        .verify_oneshot(signature.as_bytes(), data)
        .map_err(|_| Error::Crypto)?
    {
        Ok(())
    } else {
        Err(Error::Crypto)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::{sign_with_hash_alg, verify_signature};
    use crate::keys::PrivateKeyWithHashAlg;
    use ssh_encoding::Decode;
    use std::sync::Arc;

    fn key() -> ssh_key::PrivateKey {
        let pem = Rsa::generate(2048).unwrap().private_key_to_pem().unwrap();
        crate::keys::decode_secret_key(std::str::from_utf8(&pem).unwrap(), None).unwrap()
    }

    #[test]
    fn openssl_rsa_authentication_interoperates_with_rustcrypto_and_rejects_tampering() {
        let key = Arc::new(key());
        let pair = key.key_data().rsa().unwrap();
        for hash in [HashAlg::Sha256, HashAlg::Sha512] {
            let selected = PrivateKeyWithHashAlg::new(key.clone(), Some(hash));
            let bytes = sign_with_hash_alg(&selected, b"authentication exchange").unwrap();
            let signature = Signature::decode(&mut bytes.as_slice()).unwrap();
            signature::Verifier::verify(key.public_key(), b"authentication exchange", &signature)
                .unwrap();
            verify_signature(key.public_key(), b"authentication exchange", &signature).unwrap();
            assert!(verify_signature(key.public_key(), b"tampered", &signature).is_err());
            let other =
                signature::Signer::try_sign(&(pair, Some(hash)), b"authentication exchange")
                    .unwrap();
            verify(pair.public(), b"authentication exchange", &other).unwrap();
            assert_eq!(signature, other);
            let bad =
                Signature::new(signature.algorithm(), vec![0; signature.as_bytes().len()]).unwrap();
            assert!(verify(pair.public(), b"authentication exchange", &bad).is_err());
            let short = Signature::new(signature.algorithm(), vec![1]).unwrap();
            assert!(verify(pair.public(), b"authentication exchange", &short).is_err());
        }
        assert!(sign(pair, None, b"no sha1").is_err());
        let sha1 = Signature::new(Algorithm::Rsa { hash: None }, vec![0; 256]).unwrap();
        assert!(verify(pair.public(), b"no sha1", &sha1).is_err());
        let non_rsa = Signature::new(Algorithm::Ed25519, vec![0; 64]).unwrap();
        assert!(verify(pair.public(), b"wrong algorithm", &non_rsa).is_err());
    }

    #[test]
    fn openssl_rsa_rejects_mismatched_private_and_public_components() {
        let first = key();
        let second = key();
        let broken = ssh_key::private::RsaKeypair::new(
            second.key_data().rsa().unwrap().public().clone(),
            first.key_data().rsa().unwrap().private().clone(),
        )
        .unwrap();
        assert!(sign(&broken, Some(HashAlg::Sha256), b"reject").is_err());
    }

    #[test]
    fn openssl_rsa_preserves_public_key_admission_limits() {
        let public = |e: &[u8], n: &[u8]| {
            ssh_key::public::RsaPublicKey::new(
                Mpint::from_positive_bytes(e),
                Mpint::from_positive_bytes(n),
            )
            .unwrap()
        };
        let maximum = [0xff; 1024];
        assert!(check_public(&public(&[1, 0, 1], &maximum)).is_ok());
        for rejected in [
            public(&[1, 0, 1], &[0xff; 1025]),
            public(&[2, 0, 0, 0, 1], &maximum),
            public(&[1], &maximum),
            public(&[4], &maximum),
            public(&[1, 0, 1], &[2]),
        ] {
            assert!(check_public(&rejected).is_err());
        }
    }
}
