"""
    load_acs_data_api(year::Int, census_api_key; output_path::String = tempname())

Pull the raw ACS commuting flow data directly from the Census website for a given year.

## Arguments

- `year::Int`: The year of the ACS data to load.
- `census_api_key::String`: The Census API key.

## Optional Arguments

- `output_path::String`: The path to save the downloaded ACS data. Default is a temporary directory.

## Returns

Return a DataFrame with the commuting flow data for the specified year, enriched 
with income data from the CPS.

## Data Source

The ACS commuting flow data is downloaded from the Census website:

```
https://www2.census.gov/programs-surveys/demo/tables/metro-micro/{year}/commuting-flows-{year}/table1.xlsx
```
"""
function load_acs_data_api(
    info::Dict;
    output_path::String = tempname(),
)

    acs_info = info["data"]["acs"]

    year = 2020 # Currently hard coded, need to update to be dynamic
    #year = get(acs_info, "years", [2020])



    output_path = isabspath(output_path) ? output_path : joinpath(pwd(), output_path)

    if !isdir(output_path)
        mkpath(output_path)
    end

    url = "https://www2.census.gov/programs-surveys/demo/tables/metro-micro/$year/commuting-flows-$year/table1.xlsx"

    file_path = Downloads.download(url, joinpath(output_path,"acs_data.xlsx"))

    cps_data = Dict()
    cps_data[year] = WiNDCHousehold.retrieve_cps_data(year, info)

    income_2020 = leftjoin(
        cps_income(cps_data) |>
            x -> subset(x, :source => ByRow(==("hwsval"))),
        cps_numhh(cps_data),
        on = [:year, :state, :hh]
    ) |>
    dropmissing |>
    x -> groupby(x, [:year, :state]) |>
    x -> combine(x,
        [:value, :numhh] => ((a,b) -> sum(a)/(sum(b)*1e6)) => :wages
    )
        
    X = XLSX.readdata(file_path, "Table 1", "A9:J122343")

    column_names = [
        "home_fips",
        "home_cntyfips",
        "home_state",
        "home_county",
        "work_fips",
        "work_cntyfips",
        "work_state",
        "work_county",
        "workers",
        "error"
    ]

    df = DataFrame(X, column_names) |>
        x -> groupby(x, [:home_state, :work_state]) |>
        x -> combine(x, :workers => sum => :value) |>
        x -> innerjoin(
            x,
            income_2020,
            on = :home_state => :state
        ) |>
        x -> transform(x,
            [:value, :wages] => ByRow(*) => :value
        ) |>
        x -> select(x, [:home_state, :work_state, :value]) |>
        x -> subset(x,
            [:home_state, :work_state] => ByRow((hs, ws) -> hs!=ws),
            :value => ByRow(>(1))
        )


    return df

end