"""
    fetch_zip_data(
        url::String,
        filter_function::Function;;
        output_path::String = tempname(),
    )

Download a zip file from a given url and extract the files in the zip file that 
are in the `data` NamedTuple.

This function will throw an error if not all files in `data` are extracted.

## Required Arguments

1. `url::String`: The url of the zip file to download.
2. `filter_function::Function;`: A function that takes a string and returns a boolean.
    This function is used to filter the files in the zip file, it should return `true` 
    if the file should be extracted and `false` otherwise.


## Optional Arguments

- `output_path::String`: The path to save the extracted files. Default is a 
temporary directory. If this is not an absolute path, it will be joined with the 
current working directory.

## Output

Returns a vector of the absolute paths to the extracted files.
"""
function fetch_zip_data(
    url::String,
    filter_function::Function;
    output_path::String = tempname(),
)
    if !isabspath(output_path)
        output_path = joinpath(pwd(), output_path)
    end

    if !isdir(output_path)
        mkpath(output_path)
    end

    X = Downloads.download(url, joinpath(output_path,"tmp.zip"))
    r = ZipFile.Reader(X)

    extracted_files = String[]
    for f in r.files
        if filter_function(f.name)
            write(joinpath(output_path,f.name),read(f))
            push!(extracted_files, f.name)
        end
    end

    close(r)
    rm(X)

    return joinpath.(Ref(output_path),extracted_files)
end

"""
    clean_aggregate_medi_data(df::DataFrame)

Cleans Medicare or Medicaid aggregate data DataFrame.
"""
function clean_aggregate_medi_data(df::DataFrame)
    return df |>
        x -> subset(x, :Code => ByRow(==(1))) |>
        x -> select(x, Not(:Item, :Average_Annual_Percent_Growth, :Code, :Group, :Region_Number, :Region_Name)) |>
        dropmissing |>
        x -> rename(x, :State_Name => :state) |>
        x -> stack(x, Not(:state), variable_name=:year, value_name=:value) |>
        x -> transform(x,
            :year => ByRow(y -> parse(Int, replace(y, "Y" => ""))) => :year
        )
end


function load_medicare_data_api(
    census_api_key::String;
    url::String = "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/nationalhealthexpenddata/downloads/resident-state-estimates.zip",
    output_path::String = tempname(),
    years::UnitRange{Int} = 2009:2024,
    state_fips::DataFrame = load_state_fips()
)
    
    data = WiNDCHousehold.fetch_zip_data(url, y->endswith(y, ".CSV"); output_path="medicare_data")

    files_to_load = Dict(
        :medicare => "MEDICARE_AGGREGATE20.CSV",
        #"MEDICARE_ENROLLMENT20.CSV"
        #"MEDICARE_PER_ENROLLEE20.CSV"
        :medicaid => "MEDICAID_AGGREGATE20.CSV",
        #"MEDICAID_ENROLLMENT20.CSV"
        #"MEDICAID_PER_ENROLLEE20.CSV"
    )


    out = leftjoin(
        clean_aggregate_medi_data(
            CSV.read(joinpath("medicare_data",files_to_load[:medicare]), DataFrame)
        ) |> x -> rename(x, :value => :medicare),
        clean_aggregate_medi_data(
            CSV.read(joinpath("medicare_data",files_to_load[:medicaid]), DataFrame)
        ) |> x -> rename(x, :value => :medicaid),
        on=[:state, :year]
    ) |>
    x -> stack(x, [:medicare, :medicaid])

    max_year = maximum(out.year)
    for new_year in filter(y -> y > max_year, years)
        X = out |>
            x -> subset(x, :year => ByRow(==(max_year))) |>
            x -> transform(x,
                :year => ByRow(y -> new_year) => :year
            )
    
        out = vcat(out, X)
    end



    acs_medicare = load_acs_medicare_data(census_api_key; years=years, state_fips=state_fips)

    out = innerjoin(
            out,
            acs_medicare,
            on = [:state, :year]
        ) |>
        x -> transform(x,
            [:share, :value] => ByRow((s,v) -> s*v/1_000) => :final_value
        ) |>
        x -> select(x, :state, :year, :income, :variable, :final_value => :value)


    return out

end


"""
    load_acs_medicare_data(
        census_api_key::String;
        years::Vector{Int} = 2009:2024,
        state_fips::DataFrame = load_state_fips()
    )

Load ACS Medicare data from the Census API for the specified years.

## Required Arguments

- `census_api_key::String`: The Census API key.

## Optional Arguments

- `years::Vector{Int}`: The years of the ACS data to load. Default is 2009 to 2024.
- `state_fips::DataFrame`: A DataFrame containing state FIPS codes. Default is
    the result of `load_state_fips()`.

!!! note "Excluded 2020"
    The year 2020 is excluded from the default years because the ACS data for that year
    does not include Medicare data due to low response rates. 

!!! note "Early years?"
    Currently we don't have data before 2009, so requesting years before. We should
    update this to just use 2009 data for earlier years.

## Return

Returns a DataFrame with shares of income groups for Medicare enrollment by state 
and year.
"""
function load_acs_medicare_data(
    census_api_key::String;
    years::UnitRange{Int} = 2009:2024,
    state_fips::DataFrame = load_state_fips()
)
    acs_medicare = DataFrame()

    vars = Dict(
        "B27015_005E" => :hh1,#Symbol("<25k"),
        "B27015_010E" => :hh2,#Symbol("25-50k"),
        "B27015_015E" => :hh3,#Symbol("50-75k"),
        "B27015_020E" => :hh4,#Symbol( "75-100k"),
        "B27015_025E" => :hh5,#Symbol( ">100k"),
    )

    for year in union(filter(y -> y != 2020, years))
        url = "https://api.census.gov/data/$year/acs/acs1"

        query = Dict(
            :key => census_api_key,
            :for => "state:*",
            :get => join(keys(vars), ","),
        )


        response = HTTP.get(url, query = query);
        response_text = String(response.body)
        data = JSON.parse(response_text) 
        
        col_names(vars, x) = get(vars, x, Symbol(x))


        df = DataFrame([Tuple(d) for d in data[2:end]], col_names.(Ref(vars), data[1])) |>
            x -> transform(x,
                :state => ByRow(y -> year) => :year,
            )

        acs_medicare = vcat(acs_medicare, df)
    end

    acs_medicare =  acs_medicare |>
                x -> stack(x, Not(:state, :year), variable_name = :income) |>
                x -> transform(x,
                    :state => ByRow(y -> parse(Int, y)) => :state_fips,
                    :value => ByRow(y -> parse(Float64, y)) => :value
                ) |>
                x -> select(x, Not(:state)) |>
                x -> innerjoin(
                    x,
                    state_fips,
                    on = :state_fips => :fips
                ) |>
                x -> select(x, :state, :year, :income, :value) |>
                x -> groupby(x, [:state, :year]) |>
                x -> combine(x,
                    :income => identity => :income,
                    :value => (y -> y./sum(y)) => :share
                )

    acs_2020 = acs_medicare |>
        x -> subset(x, 
            :year => ByRow(∈([2019, 2021]))
        ) |>
        x -> groupby(x, [:state, :income]) |>
        x -> combine(x, :share => (y -> sum(y)/length(y)) => :share) |>
        x -> transform(x, :state => ByRow(y->2020) => :year)

    return vcat(acs_medicare, acs_2020)
end



"""
    load_medicare_data(path::String; years::UnitRange{Int}=2009:2024)

Load Medicare or Medicaid data from a CSV file at the given path.

## Required Arguments

- `path::String`: The path to the CSV file.

## Optional Arguments

- `years::UnitRange{Int}`: The range of years to include in the data. Default is 2009 to 2024.
"""
function load_medicare_data(path::String; years::UnitRange{Int}=2009:2024)
    df = CSV.read(path, DataFrame) |>
        x -> stack(x, [:medicare, :medicaid]) |>
        x -> subset(x,
            :year => ByRow(y -> y in years)
        )

    min_year = minimum(df.year)
    max_year = maximum(df.year)

    late_years = filter(y -> y > max_year, years)
    for new_year in late_years
        X = df |>
            x -> subset(x, :year => ByRow(==(max_year))) |>
            x -> transform(x,
                :year => ByRow(y -> new_year) => :year
            )
    
        df = vcat(df, X)
    end

    early_years = filter(y -> y < min_year, years)
    for new_year in early_years
        X = df |>
            x -> subset(x, :year => ByRow(==(min_year))) |>
            x -> transform(x,
                :year => ByRow(y -> new_year) => :year
            )
    
        df = vcat(df, X)
    end

    return df
end