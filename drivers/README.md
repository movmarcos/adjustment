# Drivers

`snowflake-jdbc-4.3.4.jar` (102 MB) exceeds GitHub's 100 MB per-file limit, so
it is committed here in 45 MB parts. Reassemble it on a machine that can reach
this repo but not the driver download site.

## Reassemble

macOS / Linux:

```sh
cd drivers
cat snowflake-jdbc-4.3.4.jar.part* > snowflake-jdbc-4.3.4.jar
shasum -a 256 -c snowflake-jdbc-4.3.4.jar.sha256
```

Windows (PowerShell):

```powershell
cd drivers
Get-Content snowflake-jdbc-4.3.4.jar.part* -Raw -AsByteStream | Set-Content snowflake-jdbc-4.3.4.jar -AsByteStream
(Get-FileHash snowflake-jdbc-4.3.4.jar -Algorithm SHA256).Hash.ToLower()
# compare with the hash in snowflake-jdbc-4.3.4.jar.sha256
```

Windows (cmd):

```bat
cd drivers
copy /b snowflake-jdbc-4.3.4.jar.part00 + snowflake-jdbc-4.3.4.jar.part01 + snowflake-jdbc-4.3.4.jar.part02 snowflake-jdbc-4.3.4.jar
certutil -hashfile snowflake-jdbc-4.3.4.jar SHA256
```

## Re-split (if the driver is ever replaced)

```sh
split -b 45m -d -a 2 snowflake-jdbc-<ver>.jar drivers/snowflake-jdbc-<ver>.jar.part
shasum -a 256 snowflake-jdbc-<ver>.jar > drivers/snowflake-jdbc-<ver>.jar.sha256
```

The assembled `*.jar` is git-ignored; only the parts and the checksum are tracked.
