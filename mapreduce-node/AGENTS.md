# Workspace Exclusions
- Never read, search, or inspect files inside `target/`, `.idea/`, `*.log` (including `pf-error.log`), or `*.jar` archives.
- Inspect only source files under `src/`, `pom.xml`, `Dockerfile`, `Docker-compose.yml`, and `README.md`.
# Terminal Output Limits
- Run quiet Maven commands (`mvn test -q` or `mvn compile -q`) whenever possible.
- Pipe verbose PowerShell output through `Select-Object -Last 40` so stdout never exceeds 40 lines.