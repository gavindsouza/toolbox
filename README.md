## Toolbox

Optimization & Maintenance tool for your Frappe sites

Upcoming Features:

 - [ ] Record queries & update indexes over tables automatically based on usages
 - [ ] Check table(s) health, ghost data, dangling columns (after_migrate hooks + on demand from UI/CLI)
 - [ ] Audit backup quality & raise concerns if any
 - [ ] Resource utilization & optimization (Check queue utilizations, CPU usages - recommend number of queues, scheduler ticks, other config keys?)
 - [ ] Audit apps for security & bugs (frappe:semgrep_rules, press:publishing rules, check whitelisted APIs [allowed guests], etc)
 - [ ] Dashboard with system overview, suggestions for actions
 - [ ] Track suspicious user activity (PermissionError raises, Activity & Access Log summaries, etc)
 - [ ] Show most accessed data & possible permission gaps (eg: specific users cant see list view but can see documents, DocTypes Guests can access & have permission to modify, etc)

#### License

Copyright © 2023, [Gavin D'souza](https://github.com/gavindsouza) [me@gavv.in, gavin18d@gmail.com].

ToolBox is released under "Restricted Use" License. Refer to [LICENSE](LICENSE) for full information.
