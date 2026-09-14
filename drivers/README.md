# Drivers

`snowflake-jdbc-4.3.4.jar` (102 MB) exceeds GitHub's 100 MB per-file limit, so
it is committed here in 45 MB parts. Reassemble it on a machine that can reach
this repo but not the driver download site.

## Reassemble

macOS / Linux:

```sh
cd drivers
cat snowflake-jdbc-4.3.4.jar.part* > snowflake-jdbc-4.3.4.jar
shasum -a 256 snowflake-jdbc-4.3.4.jar
cat snowflake-jdbc-4.3.4.jar.sha256
# the two hashes must match:
# b2556c6fa200aa1a7839c42a8b2ef7a11f104018c7638c8c6f40e42dee0b409b
```

Run every command from inside the `drivers` folder. `shasum -c` is not
used on purpose: a Windows checkout with CRLF line endings breaks its parsing.

Windows (PowerShell):

```powershell
cd drivers
Get-Content snowflake-jdbc-4.3.4.jar.part* -Raw -AsByteStream | Set-Content snowflake-jdbc-4.3.4.jar -AsByteStream
(Get-FileHash snowflake-jdbc-4.3.4.jar -Algorithm SHA256).Hash.ToLower()
# must print b2556c6fa200aa1a7839c42a8b2ef7a11f104018c7638c8c6f40e42dee0b409b
```

Windows (cmd):

```bat
cd drivers
copy /b snowflake-jdbc-4.3.4.jar.part00 + snowflake-jdbc-4.3.4.jar.part01 + snowflake-jdbc-4.3.4.jar.part02 snowflake-jdbc-4.3.4.jar
certutil -hashfile snowflake-jdbc-4.3.4.jar SHA256
rem must print b2556c6fa200aa1a7839c42a8b2ef7a11f104018c7638c8c6f40e42dee0b409b
```

## Re-split (if the driver is ever replaced)

```sh
split -b 45m -d -a 2 snowflake-jdbc-<ver>.jar drivers/snowflake-jdbc-<ver>.jar.part
shasum -a 256 snowflake-jdbc-<ver>.jar > drivers/snowflake-jdbc-<ver>.jar.sha256
```

The assembled `*.jar` is git-ignored; only the parts and the checksum are tracked.
