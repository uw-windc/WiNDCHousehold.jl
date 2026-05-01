# Disaggregation Data

This section describes the input data used for disaggregation in the `WiNDCHousehold` module. The Household data is disaggregated from the [`WiNDCRegional`](https://github.com/uw-windc/WiNDCRegional.jl) module using various data sources.

The data is currently assembled using a mix of local data and API calls to government data sources. The data is provided in two files which can be downloaded here:

1. [The raw household disaggregation data](https://drive.google.com/file/d/1dYt5wIbv8SKjxKx-1Ehk9EsoNDJuR_j-/view?usp=sharing) - This needs to be extracted.
2. [The state data file](https://drive.google.com/file/d/1VNThE8YUyMCxzJm9scNqn7iJL6MtHnZ0/view?usp=sharing)

## YAML Configuration

The data files are specified in a YAML configuration file named `household.yaml`. This file contains paths to the data files and any necessary API keys for accessing government data sources.

This file is structured as follows. All available years are included in this file, but can be updated as needed:

```yaml
metadata:
  title: Household Data Configuration
  description: Configuration file for household data sources
  census_api_key: census_api_key_here
  bea_api_key: bea_api_key_here
  save_data: true
  maps:
    state_map:
    windc_naics_map:
 years:
    - 2024
    - 2023
    - 2022
    - 2021
    - 2020
    - 2019
    - 2018
    - 2017
    - 2014
    - 2011
data:
  state_table:
    path: "state_data_2024.jld2"
  cps:
    api: true
    path: 'path/to/cps_directory'
    cps_identifiers:
      - gestfips  # state fips
      - a_exprrp  # expanded relationship code
      - h_hhtype  # type of household interview
      - pppos     # person identifier
      - marsupwt  # asec supplement final weight
    cps_variables:
      - hwsval    # "wages and salaries"
      - hseval    # "self-employment (nonfarm
      - hfrval    # "self-employment farm"
      - hucval    # "unemployment compensation
      - hwcval    # "workers compensation"
      - hssval    # "social security"
      - hssival   # "supplemental security"
      - hpawval   # "public assistance or wel
      - hvetval   # "veterans benefits"
      - hsurval   # "survivors income"
      - hdisval   # "disability"
      - hintval   # "interest"
      - hdivval   # "dividends"
      - hrntval   # "rents"
      - hedval    # "educational assistance"
      - hcspval   # "child support"
      - hfinval   # "financial assistance"
      - hoival    # "other income"
      - htotval   # "total household income
    cps_post2019_variables:
      - hdstval   # "retirement distributions"
      - hpenval   # "pensions and annuities"
      - hannval   # "annuities"
    cps_pre2019_variables:
      - hretval   # "retirement income"
    income_bounds:
      hh1: 25000
      hh2: 50000
      hh3: 75000
      hh4: 150000
  nipa:
    api: true
    years: 
      - 2024
  acs:
    api: true
    years: 
      - 2020
  medicare:
    api: true
    min_year: 2009
    max_year: 2024
    url: 'https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/nationalhealthexpenddata/downloads/resident-state-estimates.zip'
    files:
      medicare: "MEDICARE_AGGREGATE20.CSV"
      medicaid: "MEDICAID_AGGREGATE20.CSV"
  capital_tax_rates:
    api: false
    path: 'path/to/capital_tax_rates.csv'
  labor_tax_rates:
    api: false
    path: 'path/to/labor_tax_rates.csv'
  income_elasticities:
    api: false
    path: 'path/to/cex_income_elasticities.csv'
  windc_pce_share:
    api: false
    path: 'path/to/pce_shares.csv'
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
WiNDCHousehold.household_raw_data
```


