
"""
    load_cps_data(cps_info::Dict; state_fips = WiNDCHousehold.load_state_fips())

Load CPS data based on the provided configuration. This will either retrieve data
from the Census API or load it from a local CSV file.

## Arguments

- `info::Dict`: The household configuration dictionary, obtained from 
[`WiNDCHousehold.load_household_yaml`](@ref).

## Optional Arguments

- `state_fips::DataFrame`: A DataFrame containing state FIPS codes. Defaults to loading [`WiNDCHousehold.load_state_fips`](@ref).

## Returns

A NamedTuple with the following DataFrames:

- `income::DataFrame`: Output from 
- `numhh::DataFrame`: A DataFrame containing the number of households by household type
"""
function load_cps_data(info::Dict, years::Vector{Int}; state_fips = WiNDCHousehold.load_state_fips())

    cps_info = info["data"]["cps"]
    cps_info["census_api_key"] = get(info["metadata"],"census_api_key",nothing)
    bounds = cps_info["income_bounds"]

    api = get(cps_info, "api", false)
    path = get(cps_info, "path", nothing)

    api || !isnothing(path) || error("Either API access or a local path must be provided for CPS data.")


    if api
        cps_raw_data = api_load_cps_data(cps_info, years)
    else
        cps_raw_data = local_load_cps_data(path, years)
    end

    cps_data = clean_cps_data.(cps_raw_data, Ref(state_fips); bounds = bounds)

    #return cps_data

    #input_data = retrieve_cps_data(info, years; state_fips = state_fips)

    income = cps_income(cps_data)
    numhh = cps_numhh(cps_data)


    return (income = income, numhh = numhh)
end

# =================
# API Loading
# =================

function my_parse(val)
    if contains(val,'.')
        return parse.(Float64,val)
    end
    return parse.(Int,val)
end

"""
    get_cps_data_api(year, vars, api_key; filters = Dict())

Pull the raw data directly from the API. Return a DataFrame.
The columns of the dataframe are determined by the `vars` argument, which is a vector 
of variable names to retrieve from the API. Also adds a `year` column to the resulting DataFrame.
"""
function get_cps_data_api(year::Int, variables::Vector{String}, api_key::String; filters = Dict())

    vars = join(uppercase.(variables), ',')

    # CPS ASEC data lags by one year, so need to add 1 to the year when querying the API
    url = "https://api.census.gov/data/$(year+1)/cps/asec/mar?get=$vars&for=state:*&key=$(api_key)"

    for (variable, value) in filters
        var = uppercase(variable)
        val = join(value, ",")
        url *= "&$var=$val"
    end

    response = HTTP.get(url);
    response_text = String(response.body)
    data = JSON.parse(response_text);

    df = DataFrame([Tuple(my_parse.(row)) for row in data[2:end]], Symbol.(lowercase.(data[1]))) 
    df[!, :year] .= year
    return df
end

"""
    api_load_cps_data(cps_info::Dict, years::Vector{Int})

Load CPS data for the specified years using the Census API. This function iterates 
over the specified years, retrieves the data for each year using `get_cps_data_api`, 
and combines the results into a single vector of DataFrames.
## Arguments

- `cps_info::Dict`: The CPS configuration dictionary, which should include:
    - `census
    - api_key::String`: The Census API key.
    - `cps_identifiers::Vector{String}`: A vector of CPS identifier variables
    - `cps_variables::Vector{String}`: A vector of CPS variables to retrieve.
    - `cps_pre2019_variables::Vector{String}`: A vector of CPS
        variables to retrieve for years before 2019.
    - `cps_post2019_variables::Vector{String}`: A vector of CPS
        variables to retrieve for years 2019 and later.
    - `income_bounds::Dict{String, Int}`: A dictionary defining income bounds for
        household labels. Defaults to:
        - `hh1`: 25000
        - `hh2`: 50000
        - `hh3`: 75000
        - `hh4`: 150000

## Returns

A vector of DataFrames, where each DataFrame contains the CPS data for a specific 
year, with columns corresponding to the requested variables and additional columns 
for `year`.
"""
function api_load_cps_data(cps_info::Dict, years::Vector{Int})
    api_key = cps_info["census_api_key"]

    cps_id = lowercase.(get(cps_info, "cps_identifiers", []))
    cps_filters = get(cps_info, "cps_filters", Dict())
    cps_vars = lowercase.(get(cps_info, "cps_variables", []))
    cps_pre2019 =  lowercase.(get(cps_info, "cps_pre2019_variables", []))
    cps_post2019 = lowercase.(get(cps_info, "cps_post2019_variables", []))
    bounds = get(cps_info, "income_bounds", Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000))

    out = []

    for year in years
        variables = year + 1 < 2019 ? vcat(cps_id, cps_vars, cps_pre2019) : vcat(cps_id,cps_vars, cps_post2019)

        df = get_cps_data_api(year, variables, api_key; filters = cps_filters) |>
            x -> select(x, variables, :year) 

        out = push!(out, df)
    end
    
    return out
end


# ===================
# Local Loading
# ===================

"""
    local_load_cps_data(path::String, years::Vector{Int})

Load CPS data from local CSV files. The function expects a JSON file named 
`cps.json` in the specified path, which should contain a mapping of years to 
their corresponding CSV file names. The function reads the CSV files for the 
specified years and combines them into a single vector of DataFrames.

## Arguments

- `path::String`: The path to the directory containing the CPS data files. The directory must contain a `cps.json` file that maps years to CSV file names.
- `years::Vector{Int}`: A vector of years for which to load the CPS data.

## Returns

A vector of DataFrames, where each DataFrame contains the CPS data for a specific year, with columns corresponding to the variables in the CSV files and an additional `year` column.
"""
function local_load_cps_data(path::String, years::Vector{Int})

    isfile(joinpath(path, "cps.json")) || error(
        "CPS JSON information file not found at $(joinpath(path, "cps.json")). " *
        "Please ensure the file exists and the path is correct. Alternatively, set" *
        "`api=true` in the household configuration to load data directly from the Census API.")

    cps_json = JSON.parsefile(joinpath(path, "cps.json"))

    out = []
    for year in years
        file = get(cps_json, string(year), nothing)
        if isnothing(file)
            @warn "CPS data file for year $year not found in JSON. Skipping this year."
            continue
        end
        df = CSV.read(joinpath(path, file), DataFrame) |>
            x -> subset(x, :year => ByRow(y -> y in years))
        push!(out, df)
    end

    return out

end



# ===================
# Data Cleaning
# ===================

function clean_cps_data(cps_raw_data::DataFrame, state_fips::DataFrame; bounds = Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000))

    cps_data = cps_raw_data |>
        x -> transform(x, 
            :htotval =>  ByRow(y -> household_labels(y; bounds = bounds)) => :hh,
        ) |>
        x -> leftjoin(x, state_fips, on = :gestfips => :fips) |>
        x -> select(x, Not(:gestfips)) |>
        x -> stack(x, Not(:hh, :year, :state, :marsupwt), variable_name = :source, value_name = :value)

    return cps_data
end


"""
    household_labels(amount::Real; bounds::Dict = Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000))

Assign a household income category label based on the provided income amount and bounds.

## Arguments

- `amount::Real`: The income amount to categorize.

## Optional Arguments

- `bounds::Dict{String, Int}`: A dictionary defining income bounds for household labels. Defaults to:
    - `hh1`: 25000
    - `hh2`: 50000
    - `hh3`: 75000
    - `hh4`: 150000

These bound can be adjusted in the `household.yaml` file. See 
[`WiNDCHousehold.load_household_yaml`](@ref) for more details.

## Returns

- `String`: The household income category label (`hh1`, `hh2`, `hh3`, `hh4`, or `hh5`).
"""
function household_labels(amount::Real; bounds::Dict = Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000))

    sort(collect(keys(bounds))) == ["hh1", "hh2", "hh3", "hh4"] ||
        error("Bounds dictionary must have keys: hh1, hh2, hh3, hh4")

    household = Dict(k => val - amount for (k,val) in bounds if amount <= val) |>
        x -> !isempty(x) ? minimum(x)[1] : "hh5"

    return household
end


# ===================
# Output Functions
# ===================


"""
    cps_income(cps_raw_data::Vector{DataFrame})

Compute total CPS income by household type, state, year, and source variable.

## Arguments

- `cps_raw_data::Vector{DataFrame}`: A vector of DataFrames containing the raw CPS data.

## Returns

- `DataFrame`: A DataFrame with columns:
    - `hh::String`: Household income category.
    - `state::Int`: State code.
    - `year::Int`: The year of the data.
    - `source::String`: The source variable name.
    - `value::Float64`: The total income value in billions of dollars.

"""
function cps_income(cps_raw_data::Vector{DataFrame})

    df = vcat(cps_raw_data...) |>
        x -> transform(x, 
            [:marsupwt, :value] => ByRow(*) => :value
        ) |>
        x -> groupby(x, [:hh, :state, :year, :source]) |>
        x -> combine(x, :value => (y -> sum(y)/1e9) => :value)

    return df


end


"""
    cps_numhh(cps_raw_data::Vector{DataFrame})

Compute the number of households (in millions) by household type, state, and year from CPS raw data.

## Arguments

- `cps_raw_data::Vector{DataFrame}`: A vector of DataFrames containing the raw CPS data.

## Returns

- `DataFrame`: A DataFrame with columns:
    - `hh::String`: Household income category.
    - `state::Int`: State code.
    - `year::Int`: The year of the data.
    - `numhh::Float64`: The number of households in millions.
"""
function cps_numhh(cps_raw_data::Vector{DataFrame})
    return cps_raw_data |>
        x -> select.(x, :hh, :year, :state, :marsupwt) |>
        x -> vcat(x...) |>
        x -> groupby(x, [:hh, :state, :year]) |>
        x -> combine(x, :marsupwt => (y -> sum(y)*1e-6) => :numhh)
end
   

function cps_numhh(cps_raw_data::DataFrame)
    return cps_raw_data |>
        x -> groupby(x, [:hh, :state, :year]) |>
        x -> combine(x, :marsupwt => (y -> sum(y)*1e-6) => :numhh)
end