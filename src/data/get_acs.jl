"""
    load_acs_data_api(year::Int, cps_income::DataFrame, cps_numhh::DataFrame; output_path::String = tempname())

Pull the raw ACS commuting flow data directly from the Census website for a given year.

## Arguments

- `year::Int`: The year of the ACS data to load.
- `cps_income::DataFrame`: The CPS income data.
- `cps_numhh::DataFrame`: The CPS household number data.

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
    info::Dict,
    cps_income::DataFrame,
    cps_numhh::DataFrame;
    output_path::String = tempname()   
)
    output_path = isabspath(output_path) ? output_path : joinpath(pwd(), output_path)
    if !isdir(output_path)
        mkpath(output_path)
    end

    acs_info = info["data"]["acs"]

    years = get(acs_info, "years", [2020])
    year = 2020

    file_path = download_acs_data(year, output_path)

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

    #X = XLSX.readdata(file_path, "Table 1", "A9:J122343")
    X = XLSX.readtable(
        file_path, 
        "Table 1", 
        "A:J"; 
        first_row = 9,
        column_labels = column_names,
        stop_in_row_function = x -> ismissing(x[:home_cntyfips])
        )





    wages = get_cps_wages(info, year)


    df = DataFrame(X, column_names) |>
        x -> groupby(x, [:home_state, :work_state]) |>
        x -> combine(x, :workers => sum => :value) |>
        x -> innerjoin(
            x,
            wages,
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

"""
    get_cps_wages(info::Dict, year::Int)

Helper function to extract average wages from the CPS data for a given year. This 
is used to enrich the ACS commuting flow data with income information.

## Arguments

- `info::Dict`: The dictionary containing metadata and data paths.
- `year::Int`: The year for which to extract the average wages.
"""
function get_cps_wages(info::Dict, year::Int)
    income, numhh = load_cps_data(info, [year])    

    wages = leftjoin(
        income |>
            x -> subset(x, :source => ByRow(==("hwsval"))),
        numhh,
        on = [:year, :state, :hh]
    ) |>
    dropmissing |>
    x -> groupby(x, [:year, :state]) |>
    x -> combine(x,
        [:value, :numhh] => ((a,b) -> sum(a)/(sum(b)*1e6)) => :wages
    )

    return wages

end


function download_acs_data(year::Int, output_path::String)
    url = "https://www2.census.gov/programs-surveys/demo/tables/metro-micro/$year/commuting-flows-$year/table1.xlsx"
    file_path = Downloads.download(url, joinpath(output_path,"acs_data_$year.xlsx"))
    return file_path
end