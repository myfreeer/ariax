# Protocol Test Fixtures

`sftp-test-host` is a public, unencrypted test key used only by private loopback
servers. It is deliberately checked in with its public key for reproducible
host-key and authentication tests.

`scripts/run-openssh-interop.py` starts a separate loopback OpenSSH process using
temporary configuration, keys and a 12-byte file. It does not change an SSH
service or user configuration. Pass the all-feature `openssh_interop` test
executable with `--test-binary`, a server executable with `--sshd`, the local
account with `--user`, and a retained server `--log`. Linux servers use
`--path-mode native`; an MSYS2 server launched from WSL uses `--path-mode msys`.
The runner creates a private client key copy and an empty private known-host
file, using mode 600 or a protected Windows ACL granting FullControl exactly
to the current client user, SYSTEM and Administrators with current-user
ownership, and explicitly passes its
fixture environment to native clients. MSYS2 daemon cleanup uses the fixture's
PID file so the native SSH listener cannot outlive its environment launcher.
