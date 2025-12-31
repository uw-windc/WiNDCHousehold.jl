"""
    load_nipa_data_api(years::Vector{Int}, bea_api_key)

Pull the raw data directly from the BEA API

## Arguments

- `years::Vector{Int}`: The years of the NIPA data to load.
- `bea_api_key::String`: The BEA API key.

## Returns

A DataFrame with the NIPA data for the specified years.

## Query 

| Variable | Value |
|----------|-------|
| Method   | GetData |
| datasetname | NIPA |
| TableName | T20100 |
| Frequency | A |
| Year      | Comma-separated list of years |
| ResultFormat | json |
"""
function load_nipa_data_api(years::Vector{Int}, bea_api_key)

    bea_url = "https://apps.bea.gov/api/data"

    query = Dict(
        "UserID" => bea_api_key,
        "Method" => "GetData",
        "datasetname" => "NIPA",
        "TableName" => "T20100",
        "Frequency" => "A",
        "Year" => join(years,","),
        "ResultFormat" => "json"
    )

    response = HTTP.get(bea_url, query = query)
    response_text = String(response.body)
    data = JSON.parse(response_text);

    df = DataFrame(data["BEAAPI"]["Results"]["Data"]) |>
        x -> rename(x, :TimePeriod => :year, :DataValue => :value) |>
        x -> transform(x, 
            :year => ByRow(y -> parse(Int, y)) => :year,
            :value => ByRow(y -> parse(Float64, replace(y, "," => ""))) => :value
        ) 

    return df
end


"""
    cps_vs_nipa_income_categories(cps_income::DataFrame, nipa::DataFrame)

Compare CPS income data to NIPA data by income categories.

"""
function cps_vs_nipa_income_categories(cps_income::DataFrame, nipa::DataFrame)

    nipa_cps_link = DataFrame([
        (LineNumber = "1",  source = "totinc",  category = "total_income"),
        (LineNumber = "3",  source = "hwsval",  category = "wages and salaries"),
        (LineNumber = "10", source = "hfrval",  category = "proprietor's income farm"),
        (LineNumber = "11", source = "hseval",  category = "proprietor's income: non-farm"),
        (LineNumber = "12", source = "hrntval", category = "rental income"),
        (LineNumber = "14", source = "hintval", category = "personal interest income"),
        (LineNumber = "15", source = "hdivval", category = "personal dividend income"),
        (LineNumber = "18", source = "hssval",  category = "government benefits: social security"),
        (LineNumber = "18", source = "hssival", category = "government benefits: social security"),
        (LineNumber = "18", source = "hdisval", category = "government benefits: social security"),
        (LineNumber = "21", source = "hucval",  category = "government benefits: unemployment insurance"),
        (LineNumber = "22", source = "hvetval", category = "government benefits: veterans' benefits"),
        (LineNumber = "23", source = "hwcval",  category = "government benefits: other"),
        (LineNumber = "23", source = "hpawval", category = "government benefits: other"),
        (LineNumber = "23", source = "hsurval", category = "government benefits: other"),
        (LineNumber = "23", source = "hedval",  category = "government benefits: other"),
        (LineNumber = "24", source = "hcspval", category = "non-government transfer income"),
        (LineNumber = "24", source = "hfinval", category = "non-government transfer income"),
        (LineNumber = "24", source = "hoival",  category = "non-government transfer income"),
    ])



    cps_totals = cps_income |>
        x -> innerjoin(x, nipa_cps_link, on = :source) |>
        x -> groupby(x, [:year, :LineNumber, :category]) |>
        x -> combine(x, :value => (y -> sum(y)*1e3) => :cps)


    return nipa |>
        x -> select(x, :year, :LineNumber, :value => :nipa) |>
        x -> innerjoin(x, cps_totals, on = [:year, :LineNumber]) |>
        x -> select(x, :year, :category, :nipa, :cps)
        

end


"""
    windc_vs_nipa_income_categories(
        state_table::WiNDCRegional.State,
        nipa::DataFrame
    )

Compare WiNDC labor and capital income to NIPA data.
"""
function windc_vs_nipa_income_categories(
        state_table::WiNDCRegional.State,
        nipa::DataFrame
    )

    ld0_kd0_windc = table(state_table, :Labor_Demand, :Capital_Demand; normalize=:Use) |>
    x -> groupby(x, [:year, :parameter]) |>
    x -> combine(x, :value => sum => :value) 


    labor_capital_map = DataFrame(
        LineNumber = ["2", "9", "12", "13"],
        parameter = [:labor_demand, :capital_demand, :capital_demand, :capital_demand]
    )

    return nipa |>
        x -> innerjoin(x, labor_capital_map, on = :LineNumber) |>
        x -> groupby(x, [:year, :parameter]) |>
        x -> combine(x, :value => sum => :nipa) |>
        x -> leftjoin(x, ld0_kd0_windc, on = [:year, :parameter]) |>
        x -> transform(x, :value => ByRow(y -> y*(1e3)) => :windc) |>
        x -> select(x, Not(:value))

end


"""
    nipa_fringe_benefit_markup(nipa::DataFrame)

Calculate the fringe benefit markup from NIPA data.
"""
function nipa_fringe_benefit_markup(nipa::DataFrame)

    return nipa |>
        x -> subset(x, :LineNumber => ByRow(∈(["2","3"]))) |>
        x -> select(x, :year, :LineNumber, :value) |>
        x -> unstack(x, :LineNumber, :value) |>
        x -> rename(x, 2 => :compensation, 3 => :wages) |>
        x -> transform(x,
            [:compensation, :wages] => ByRow(/) => :markup
        ) |>
        x -> select(x, :year, :markup)

end