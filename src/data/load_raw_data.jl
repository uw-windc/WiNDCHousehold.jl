"""
    load_household_yaml(path::String)
    load_household_yaml()

Load the household YAML configuration file from the specified path.

!!!note ""
    This is a placeholder documentation string. More details to be added later.
"""
function load_household_yaml(path::String)
    path = joinpath(path)
    return YAML.load_file(path)
end


function load_household_yaml()
    return load_household_yaml("household.yaml")
end

function household_raw_data()
    info = load_household_yaml()
    return household_raw_data(info)
end

function household_raw_data(path::String)
    info = load_household_yaml(path)
    return household_raw_data(info)
end

"""
    household_raw_data()
    household_raw_data(path::String)
    household_raw_data(info::Dict)

Load the raw household data based on the provided configuration dictionary.

## Arguments

If no arguments are provided, the function loads the configuration from `household.yaml` 
in the current directory. If a `path::String` is provided, it loads the configuration from the specified path.
If a `info::Dict` is provided, it uses the given configuration dictionary directly.

## Returns

This currently returns a tuple containing:

- A `State` table object
- A [`WiNDCHousehold.RawHouseholdData`](@ref) object containing all the raw household data.

## Raw Data Loaded

- [`WiNDCHousehold.load_cps_data`](@ref)
- [`WiNDCHousehold.load_nipa_data_api`](@ref)
- [`WiNDCHousehold.load_acs_data_api`](@ref)
- [`WiNDCHousehold.load_medicare_data_api`](@ref)
- [`WiNDCHousehold.load_labor_tax_rates`](@ref)
- [`WiNDCHousehold.load_capital_tax_rates`](@ref)
- [`WiNDCHousehold.load_cex_income_elasticities`](@ref)
- [`WiNDCHousehold.load_pce_shares`](@ref)

## Maps Loaded

- [`WiNDCHousehold.load_state_fips`](@ref)
- [`WiNDCHousehold.load_cps_income_categories`](@ref)

"""
function household_raw_data(info::Dict)

    bea_api_key = info["metadata"]["bea_api_key"]
    census_api_key = info["metadata"]["census_api_key"]
    years = info["metadata"]["years"]

    @load info["data"]["state_table"]["path"] state_table

    

    state_fips = WiNDCHousehold.load_state_fips()
    income_categories = WiNDCHousehold.load_cps_income_categories()

    income, numhh = load_cps_data(info)
    nipa = load_nipa_data_api(years, bea_api_key)


    acs = WiNDCHousehold.load_acs_data_api(info)#2020, census_api_key)

    medicare_min_year = info["data"]["medicare"]["min_year"]
    medicare_max_year = info["data"]["medicare"]["max_year"]
    medicare_url = info["data"]["medicare"]["url"]
    medicare_files = info["data"]["medicare"]["files"]
    medicare = WiNDCHousehold.load_medicare_data_api(
            census_api_key; 
            url=medicare_url,
            years=medicare_min_year:medicare_max_year,
            files_to_load = medicare_files,
            )

    labor_path = info["data"]["labor_tax_rates"]["path"]
    labor_tax_rates = WiNDCHousehold.load_labor_tax_rates(labor_path)

    capital_path = info["data"]["capital_tax_rates"]["path"]
    capital_tax_rates = WiNDCHousehold.load_capital_tax_rates(capital_path)


    eta_path = info["data"]["income_elasticities"]["path"]
    eta = WiNDCHousehold.load_cex_income_elasticities(eta_path)
    pce_share_path = info["data"]["windc_pce_share"]["path"]
    pce_share = WiNDCHousehold.load_pce_shares(pce_share_path)


    HH_Raw_Data = WiNDCHousehold.RawHouseholdData(
        state_table,
        years,
        income,
        numhh,
        nipa,
        acs,
        medicare,
        labor_tax_rates,
        eta,
        pce_share,
        capital_tax_rates;
        state_fips = state_fips,
        income_categories = income_categories,
    )

    return state_table, HH_Raw_Data

end