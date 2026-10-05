Ariax experimental CLI package draft

These files are prepared from local validation artifacts. The package manifest
identifies the exact binary, feature bundle, runtime requirements, source/build
identities and notices. This is not an approved release candidate.
Path audits, independent build comparisons and package-operation results apply
only to the recorded artifacts and hosts. Fresh-OS and minimum-OS acceptance
remain open. Earlier binaries and failed audits remain historical evidence.

Ariax remains parallel to aria2; reviewed option-handler coverage is 54/207.
Do not install this draft as an aria2c replacement or infer a data migration.
Use a separate configuration and download/session directory for evaluation.

The minimal bundle enables Metalink with the HTTP baseline. Standard also
enables FTP/FTPS and SFTP. Full and compat additionally enable the native
BitTorrent adapter. The package directory name identifies your selected bundle.

Linux minimal/standard require an x86_64 GNU/Linux loader, libc.so.6, libm.so.6,
libgcc_s.so.1 and glibc 2.34 or newer. Full/compat additionally require
libstdc++.so.6, glibc 2.38 or newer and GLIBCXX 3.4.30 or newer. These system
libraries are supplied by the host distribution and are not bundled here.

Windows-GNU minimal/standard require only Windows system DLLs. Full/compat
also carry libstdc++-6.dll, libgcc_s_seh-1.dll and libwinpthread-1.dll beside
ariax.exe. Keep those files together; a developer toolchain on PATH is not
required. The manifest records the exact reviewed runtime files and licenses.

Read LICENSE, THIRD-PARTY-NOTICES.txt, DATA-NOTICE.txt, license-review.json and
the applicable licenses/ files. The notice collection is conservative and may
include unused build/source materials. Upstream terms remain in force.
