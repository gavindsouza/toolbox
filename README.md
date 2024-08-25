## Toolbox

Optimization & Maintenance tool for your Frappe sites

### Features

![toolbox-workspace](https://github.com/user-attachments/assets/1b7706d8-38cc-4028-85c2-7cdc03ede581)
![toolbox-settings](https://github.com/user-attachments/assets/7a592a26-db82-4eb6-8cbd-8c1298d0447b)

#### Dashboards

![toolbox-index-manager-dashboard](https://github.com/user-attachments/assets/23aac030-5379-497e-95fa-d1058ade6a98)
![toolbox-site-manager-dashboard](https://github.com/user-attachments/assets/a2d41b84-068b-441e-91e0-e3640030e2d2)

#### Reports

![toolbox-unused-indexes-report](https://github.com/user-attachments/assets/c6cf737b-2725-4be4-8093-801e8487e66a)

#### Planned Features

 - [ ] Check table(s) health, ghost data, dangling columns (after_migrate hooks + on demand from UI/CLI)
 - [ ] Audit backup quality & raise concerns if any
 - [ ] Resource utilization & optimization (Check queue utilizations, CPU usages - recommend number of queues, scheduler ticks, other config keys?)
 - [ ] Audit apps for security & bugs (frappe:semgrep_rules, press:publishing rules, check whitelisted APIs [allowed guests], etc)
 - [ ] Dashboard with system overview, suggestions for actions
 - [ ] Track suspicious user activity (PermissionError raises, Activity & Access Log summaries, etc)
 - [ ] Show most accessed data & possible permission gaps (eg: specific users cant see list view but can see documents, DocTypes Guests can access & have permission to modify, etc)

#### License

Copyright © 2023, [Gavin D'souza](https://github.com/gavindsouza) [me@gavv.in, gavin18d@gmail.com].

ToolBox is released under "AGPL" License. Refer to [LICENSE](LICENSE) for full information.
