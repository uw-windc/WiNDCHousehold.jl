"""
    load_labor_tax_rates(
            path::String;
            state_abbreviations = WiNDCHousehold.load_state_fips(cols_to_keep = [:state, :abbreviation])
        )

Load labor tax rates from a CSV file located at `path`. The CSV is expected to 
contain state-level labor tax rate information, which will be matched with state 
abbreviations provided by `state_abbreviations`.


"""
function load_labor_tax_rates(
        path::String;
        state_abbreviations = WiNDCHousehold.load_state_fips(cols_to_keep = [:state, :abbreviation])
    )

    return CSV.read(path, DataFrame) |>
        x -> transform(x, :state => ByRow(uppercase) => :abbreviation) |>
        x -> select(x, Not(:state)) |>
        x -> innerjoin(x, state_abbreviations, on = :abbreviation) |>
        x -> select(x, Not(:abbreviation)) |>
        x -> rename(x, :tp => :tfica) |>
        x -> stack(x, Not(:hh, :state), value_name = :labor_tax_rate) 
end


"""
    load_capital_tax_rates(
        path::String;
        state_abbreviations = WiNDCHousehold.load_state_fips(cols_to_keep = [:state, :abbreviation])
    )

Load capital tax rates from a CSV file located at `path`. The CSV is expected to
contain state-level capital tax rate information, which will be matched with state
abbreviations provided by `state_abbreviations`.
"""
function load_capital_tax_rates(
        path::String;
        state_abbreviations = WiNDCHousehold.load_state_fips(cols_to_keep = [:state, :abbreviation])
    )

    return CSV.read(path, DataFrame) |>
        x -> transform(x, :r => ByRow(uppercase) => :abbreviation) |>
        x -> select(x, Not(:r)) |>
        x -> innerjoin(x, state_abbreviations, on = :abbreviation) |>
        x -> select(x, Not(:abbreviation)) |>
        x -> rename(x, :value => :capital_tax_rate)

end