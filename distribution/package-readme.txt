Ariax experimental CLI package draft

These files are prepared from retained local validation artifacts. The package
manifest identifies the exact binary, historical evidence, feature bundle,
runtime requirements and notices. This is not an approved release candidate.
These rebuilt minimal/standard binaries pass the recorded source-path audit.
Repository, dependency-cache, target and temporary source paths are remapped.
Each bundle matches an independent build using separate target/temp directories
on the same host and shared source cache/toolchain. Reduced-environment package
checks pass on WSL1 and native Windows. Linux also loads a configured system
preload. Fresh-OS and minimum-OS acceptance remain open. Earlier binaries and
their failed audits stay historical.

Ariax remains parallel to aria2; reviewed option-handler coverage is 54/207.
Do not install this draft as an aria2c replacement or infer a data migration.
Use a separate configuration and download/session directory for evaluation.

The minimal bundle enables Metalink with the HTTP baseline. Standard also
enables FTP/FTPS and SFTP. Full and compat additionally require the native BT
adapter and have planned manifests pending artifact and runtime inspection.

The retained Linux binaries require an x86_64 GNU/Linux loader, libc.so.6,
libm.so.6, libgcc_s.so.1 and glibc 2.34 or newer. These system libraries are
provided by the host distribution and are not copied into the package.
The retained Windows-GNU binaries are x86_64 and import only Windows system
DLLs; no additional MinGW DLL is required by those two retained bundles.
These observations do not establish clean-host or minimum-OS acceptance.

Read LICENSE, THIRD-PARTY-NOTICES.txt, DATA-NOTICE.txt, license-review.json and
the applicable licenses/ files. The notice collection is conservative and may
include unused build/source materials. Upstream terms remain in force.
