"""
    load_state_fips(;
            path = joinpath(@__DIR__, "maps", "state_fips.csv"),
            cols_to_keep = [:fips, :state]
        )

Load a CSV file containing state FIPS codes and state names. The default path is
set to `maps/state_fips.csv` relative to this file's directory. You can specify
which columns to keep using the `cols_to_keep` argument.

Returns a DataFrame with the specified columns as Strings.

## Optional Arguments

- `path::String`: Path to the state FIPS CSV file.
- `cols_to_keep::Vector{Symbol}`: Columns to keep from the CSV file. Default is `[:fips, :state]`.

It is recommended to keep the columns `:fips` and `:state` for proper mapping.
"""
function load_state_fips(;
        path = joinpath(@__DIR__, "maps", "state_fips.csv"),
        cols_to_keep = [:fips, :state]
    )

    state_fips = CSV.read(
        path, 
        DataFrame,
        select = cols_to_keep
    )

    return state_fips

end

"""
    load_cps_income_categories(;
            path = joinpath(@__DIR__, "maps", "cps_income_categories.csv"),
            cols_to_keep = [:windc, :source]
        )

Load a CSV file containing CPS income category mappings. The default path is
set to `maps/cps_income_categories.csv` relative to this file's directory. You can specify
which columns to keep using the `cols_to_keep` argument.

Returns a DataFrame with the specified columns as Strings.
"""
function load_cps_income_categories(;
        path = joinpath(@__DIR__, "maps", "cps_income_categories.csv"),
        cols_to_keep = [:windc, :source]
    )

    income_categories = CSV.read(
        path,
        DataFrame,
        select = cols_to_keep
    )

    return income_categories
end

"""
    load_windc_naics_map(;
            path = joinpath(@__DIR__, "maps", "naics_windc_map.csv")
        )

Load a CSV file containing the mapping between WiNDC labels and NAICS codes.
The default path is set to `maps/naics_windc_map.csv` relative to this file's directory.

Returns a DataFrame with columns `:naics` and `:windc`.
"""
function load_windc_naics_map(;
        path = joinpath(@__DIR__, "maps", "naics_windc_map.csv")
    )

    windc_naics_map = CSV.read(
        path,
        DataFrame,
        #select = cols_to_keep
    ) |>
    x -> select(x, :bea_code => :naics,  :windc_label => :windc)
    
    return windc_naics_map
end