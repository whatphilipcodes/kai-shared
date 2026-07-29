# kai-shared

### Troubleshooting

- Windows Firewall Issues (Also when running WSL!)
  - incoming ports have to be manually unblocked. Run this in admin Powershell
    ```sh
    New-NetFirewallRule -DisplayName "Kai IPC Ports" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5556,5557
    ```