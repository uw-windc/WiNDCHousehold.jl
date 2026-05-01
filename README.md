# WiNDC Household Disaggregation

[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://uw-windc.github.io/WiNDCHousehold.jl/dev/)

This package contains functions and data structures to disaggregate household-level data for the WiNDC model.

## Basic Example

In order to run this code you will need to download the state data:

1. [The state data file](https://drive.google.com/file/d/1NTx-NTb5A635-u1OAKQc_qcsFOA7AOf3/view?usp=sharing) - Extract and note the location, you'll need to update the YAML file below.

In your working directory create a file called `household.yaml` and populate it with the contents of the [Household YAML File](#household-yaml-file) section below, making sure to update the paths to point to the files you downloaded.

Then you can run the following code:


```julia
using WiNDCHousehold

state_table, HH_Raw_Data = WiNDCHousehold.household_raw_data()

HH = WiNDCHousehold.build_household_table(
    state_table,
    HH_Raw_Data;
)
```



## Household YAML File

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


