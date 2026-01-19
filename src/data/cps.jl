

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
function load_cps_data(info::Dict; state_fips = WiNDCHousehold.load_state_fips())
    cps_info = info["data"]["cps"]
    cps_info["census_api_key"] = get(info["metadata"],"census_api_key",nothing)
    bounds = cps_info["income_bounds"]

    api = get(cps_info, "api", false)
    path = get(cps_info, "path", "")
    years = get(cps_info, "years", [2024])

    input_data = Dict()
    if api
        input_data = retrieve_cps_data(cps_info)
        if path != ""
            mkpath(path)
            for year in years
                CSV.write(joinpath(path, "cps_$(year).csv"), input_data[year])
            end
        end
    else
        for year in years
            input_data[year] = CSV.read(joinpath(path, "cps_$(year).csv"), DataFrame)
        end
    end


    for (year, df) in input_data
        input_data[year] = df |>
            x -> transform(x, 
                :htotval =>  ByRow(y -> household_labels(y; bounds = bounds)) => :hh,
                :gestfips => ByRow(y -> year) => :year,
            ) |>
            x -> leftjoin(x, state_fips, on = :gestfips => :fips) |>
            x -> select(x, Not(:gestfips))
        
    end

    income = cps_income(input_data)
    numhh = cps_numhh(input_data)


    return (income = income, numhh = numhh)
end

"""
    get_cps_data(
        year, 
        cps_rw, 
        variables, 
        api_key; bounds = Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000)
        )

Retrieve and process CPS data for a specific year. 

## Arguments

- `year::Int`: The year of the CPS data to load.
- `cps_rw::Vector{String}`: The list of CPS raw variables to load. This must contain:
    - `gestfips`: State FIPS code.
    - `a_exprrp`: Expanded relationship code.
    - `h_hhtype`: Type of household interview.
    - `pppos`: Person identifier.
    - `marsupwt`: ASEC supplement final weight.
- `variables::Vector{String}`: The list of CPS variables to process. This must contain:
    - `htotval`: Total household income.
- `api_key::String`: The Census API key. 

## Optional Arguments

- `bounds::Dict{String, Int}`: A dictionary defining income bounds for household labels. Defaults to:
    - `hh1`: 25000
    - `hh2`: 50000
    - `hh3`: 75000
    - `hh4`: 150000

These bound can be adjusted in the `household.yaml` file. See 
[`WiNDCHousehold.load_household_yaml`](@ref) for more details.

## Returns

- `DataFrame`: A DataFrame containing the columns:
    - `gestfips::Int`: State FIPS code.
    - `year::Int`: The year of the data.
    - `hh::String`: Household income category.
    - `marsupwt::Float64`: ASEC supplement final weight.
    - `source::String`: The source variable name.
    - `value::Float64`: The value of the source variable.

## Process

1. Download CPS data from the Census API using [`WiNDCHousehold.get_cps_data_api`](@ref).
2. Filter the data to include:
    - Only households with representative persons (`a_exprrp` in [1,2]).
    - Only household interviews (`h_hhtype` == 1).
    - Only person identifier 41 (`pppos` == 41).
3. Select relevant columns and stack the `variables` column into `source` and `value`
"""
function get_cps_data(year, cps_rw, variables, api_key; bounds = Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000))

    vars = join(vcat(cps_rw,variables), ',')

    given_vars,d = WiNDCHousehold.get_cps_data_api(year, vars, api_key)


    vars = Symbol.(lowercase.(given_vars))
    modify_vars = Symbol.(lowercase.(variables))

    df = DataFrame(d, vars) |>
        x -> subset(x,
            :a_exprrp => ByRow(y -> y in [1,2]), # extract the household file with representative persons
            :h_hhtype => ByRow(y -> y == 1), # extract the household file with representative persons
            :pppos => ByRow(y -> y == 41)
        ) |>
        #x -> transform(x, 
        #    :gestfips => ByRow(y -> year - 1) => :year, # Years are lagged by one
        #    :htotval =>  ByRow(y -> household_labels(y; bounds = bounds)) => :hh, # add household label to each entry
        #) |>
        x -> select(x, Not(:state, :a_exprrp, :h_hhtype, :pppos)) #|>
        #x -> stack(x, modify_vars, variable_name = :source, value_name = :value) |>
        #x -> subset(x, :value => ByRow(!=(0)))

    return df
end

"""
    retrieve_cps_data(cps_info::Dict)

Retrieve CPS data from the Census API based on the provided configuration. The data 
is retrieved using [`WiNDCHousehold.get_cps_data`](@ref) for each specified year and combined into a single DataFrame.

## Arguments

- `cps_info::Dict`: A dictionary containing CPS configuration, including:
    - `census_api_key::String`: The Census API key.
    - `years::Vector{Int}`: The years of CPS data to retrieve.
    - `cps_identifiers::Vector{String}`: The list of CPS raw variables to load.
    - `cps_variables::Vector{String}`: The list of CPS variables to process.
    - `cps_pre2019_variables::Vector{String}`: Additional variables for years before 2019.
    - `cps_post2019_variables::Vector{String}`: Additional variables for years 2019 and later.
    - `income_bounds::Dict{String, Int}`: A dictionary defining income bounds for household labels.

The `cps_info` dictionary is build by [`WiNDCHousehold.load_household_yaml`](@ref).

## Returns

A dictionary mapping each year to its corresponding CPS DataFrame. Each DataFrame
contains the columns given by the input variables.
"""
function retrieve_cps_data(cps_info::Dict)
    api_key = get(cps_info, "census_api_key", "")
    years = get(cps_info, "years", [2024])
    cps_rw = uppercase.(get(cps_info, "cps_identifiers", []))
    cps_vars = uppercase.(get(cps_info, "cps_variables", []))
    cps_pre2019 =  uppercase.(get(cps_info, "cps_pre2019_variables", []))
    cps_post2019 = uppercase.(get(cps_info, "cps_post2019_variables", []))
    bounds = get(cps_info, "income_bounds", Dict("hh1" => 25000, "hh2" => 50000, "hh3" => 75000, "hh4" => 150000))

    out = Dict()
    for year in years
        variables = year + 1 < 2019 ? vcat(cps_vars, cps_pre2019) : vcat(cps_vars, cps_post2019)
        df = get_cps_data(year + 1, cps_rw, variables, api_key; bounds = bounds)
        out[year] = df
    end

    return out
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


"""
    cps_income(cps_raw_data::Dict)

Compute total CPS income by household type, state, year, and source variable.

## Arguments

- `cps_raw_data::Dict`: Generated dataset within [`load_cps_data`](@ref).

## Returns

- `DataFrame`: A DataFrame with columns:
    - `hh::String`: Household income category.
    - `state::Int`: State code.
    - `year::Int`: The year of the data.
    - `source::String`: The source variable name.
    - `value::Float64`: The total income value in billions of dollars.

"""
function cps_income(cps_raw_data::Dict)
    return cps_raw_data |>
        data -> vcat([stack(df, Not(:hh, :year, :state, :marsupwt), variable_name = :source, value_name = :value) for (year, df) in data]...) |>
        x -> transform(x, 
            [:marsupwt, :value] => ByRow(*) => :value
        ) |>
        x -> groupby(x, [:hh, :state, :year, :source]) |>
        x -> combine(x, :value => (y -> sum(y)/1e9) => :value)
end


"""
    cps_numhh(cps_raw_data::Dict)

Compute the number of households (in millions) by household type, state, and year from CPS raw data.

## Arguments

- `cps_raw_data::Dict`: Generated dataset within [`load_cps_data`](@ref).

## Returns

- `DataFrame`: A DataFrame with columns:
    - `hh::String`: Household income category.
    - `state::Int`: State code.
    - `year::Int`: The year of the data.
    - `numhh::Float64`: The number of households in millions.
"""
function cps_numhh(cps_raw_data::Dict)
    return cps_raw_data |>
        data -> vcat([select(df, :hh, :year, :state, :marsupwt) for (year, df) in data]...) |>
        x -> groupby(x, [:hh, :state, :year]) |>
        x -> combine(x, :marsupwt => (y -> sum(y)*1e-6) => :numhh)
end

