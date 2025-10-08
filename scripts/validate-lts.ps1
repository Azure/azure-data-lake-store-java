param(
    [string]$MavenExecutable = "mvn"
)

$commands = @(
    @{ Name = "Java 8";      Args = @("clean", "compile", "-DskipTests") },
    @{ Name = "Java 11";     Args = @("-Pjava11", "clean", "compile", "-DskipTests") },
    @{ Name = "Java 17";     Args = @("-Pjava17", "clean", "compile", "-DskipTests") },
    @{ Name = "Java 21";     Args = @("-Pjava21", "clean", "compile", "-DskipTests") },
    @{ Name = "Restore Java 8"; Args = @("clean", "compile", "-DskipTests") }
)

foreach ($command in $commands) {
    Write-Host "==> Building with $($command.Name)" -ForegroundColor Cyan
    Write-Host "    $MavenExecutable $($command.Args -join ' ')" -ForegroundColor DarkGray

    & $MavenExecutable @($command.Args)
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed for $($command.Name)"
        exit $LASTEXITCODE
    }
}

Write-Host "All LTS compilation checks completed successfully." -ForegroundColor Green
