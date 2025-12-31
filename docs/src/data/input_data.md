# Disaggregation Data

This section describes the input data used for disaggregation in the `WiNDCHousehold` module. The Household data is disaggregated from the [`WiNDCRegional`](https://github.com/uw-windc/WiNDCRegional.jl) module using various data sources.

The data is currently assembled using a mix of local data and API calls to government data sources. The data is provided in two files which can be downloaded here:

1. [The raw household disaggregation data](https://drive.google.com/file/d/1dYt5wIbv8SKjxKx-1Ehk9EsoNDJuR_j-/view?usp=sharing) - This needs to be extracted.
2. [The state data file](https://drive.google.com/file/d/1VNThE8YUyMCxzJm9scNqn7iJL6MtHnZ0/view?usp=sharing)

## YAML Configuration

The data files are specified in a YAML configuration file named `household.yaml`. This file contains paths to the data files and any necessary API keys for accessing government data sources.

This file is structured as follows:

```yaml
metadata:
  title: Household Data Configuration
  description: Configuration file for household data sources
  census_api_key: census_api_key_here
  bea_api_key: bea_api_key_here
  maps:
    state_map:
    windc_naics_map:
data:
  state_table:
    path: path/to/state_data_2024.jld2
  cps:
    api: true
    years:
      - 2024
  nipa:
    api: true
    years: 
      - 2024
  acs:
    api: true
    years: 
      - 2020
    requires: cps
  medicare:
    api: true
    min_year: 2009
    max_year: 2024
  capital_tax_rates:
    api: false
    path: path/to/capital_tax_rates.csv
  labor_tax_rates:
    api: false
    path: path/to/labor_tax_rates.csv
  income_elasticities:
    api: false
    path: path/to/national_income_elasticities_CEX_2013_2017.csv
  windc_pce_share:
    api: false
    path: path/to/windc_pce_map.csv
magic_numbers:
  corporate_rate: .186
  cbo_wealth_distribution:
    hh1: 0.025871517
    hh2: 0.043989237
    hh3: 0.077542098
    hh4: 0.147248546
    hh5: 0.705348602
  bls_distribution_expenditures:
    hh1: 25138
    hh2: 36770
    hh3: 47664
    hh4: 64910
    hh5: 112221
```

## Loading the Data

```@docs
household_raw_data
```


