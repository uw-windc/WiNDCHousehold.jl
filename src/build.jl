
"""
    initialize_table(state_table::State)

Create an initial `HouseholdTable` from a `State` table by selecting only the 
relevant parameters, sets, and elements.

Essentially, all parameters, sets, and elements are maintained except those related
to `personal_consumption`.

## Parameters Maintained

- `Capital_Demand`
- `Duty`
- `Export`
- `Government_Final_Demand`
- `Household_Supply`
- `Import`
- `Intermediate_Demand`
- `Intermediate_Supply`
- `Investment_Final_Demand`
- `Labor_Demand`
- `Local_Demand`
- `Local_Margin_Supply`
- `Margin_Demand`
- `National_Demand`
- `National_Margin_Supply`
- `Output_Tax`
- `Reexport`
- `Tax`

## Sets Maintained

- `duty`   
- `export`
- `government_final_demand`
- `import`
- `investment_final_demand`
- `local_demand`
- `margin`
- `national_demand`
- `reexport`
- `sector`
- `tax`
- `trade`
- `transport`
- `state`
- `capital_demand`
- `commodity`
- `labor_demand`
- `output_tax`
- `year`
- `Final_Demand`
- `Other_Final_Demand`
- `Margin_Supply`
- `Use`
- `Supply`
- `Value_Added`


## Sets Added

- `household` 
    - Domain: `col`
    - Description: "Household Categories"
    - Elements: `hh1`, `hh2`, `hh3`, `hh4`, `hh5`
"""
function initialize_table(state_table::State)

    parameters_to_keep = [
        #:Capital_Demand
        :Duty
        :Export
        :Government_Final_Demand
        :Household_Supply
        :Import
        :Intermediate_Demand
        :Intermediate_Supply
        :Investment_Final_Demand
        :Labor_Demand
        :Local_Demand
        :Local_Margin_Supply
        :Margin_Demand
        :National_Demand
        :National_Margin_Supply
        :Output_Tax
        :Reexport
        :Tax
    ]

    sets_to_keep = [
        :duty   
        :export
        :government_final_demand
        :import
        :investment_final_demand
        :local_demand
        :margin
        :national_demand
        :reexport
        :sector
        :tax
        :trade
        :transport
        :state
        #:capital_demand
        :commodity
        :labor_demand
        :output_tax
        :year
        :Final_Demand
        :Other_Final_Demand
        :Margin_Supply
        :Use
        :Supply
        :Value_Added
    ]

    T = table(state_table, parameters_to_keep...)
    S = sets(state_table, sets_to_keep..., parameters_to_keep...) |>
        x -> vcat(x,
            DataFrame([
                (name = :household, description = "Household Categories", domain = :col),
            ])
        )
    E = elements(state_table, sets_to_keep..., parameters_to_keep...) |>
        x -> subset(x, :name => ByRow(!=(:personal_consumption))) |>
        x -> vcat(x,
            DataFrame([
                (name = :hh1, description = "Household Category 1", set = :household),
                (name = :hh2, description = "Household Category 2", set = :household),
                (name = :hh3, description = "Household Category 3", set = :household),
                (name = :hh4, description = "Household Category 4", set = :household),
                (name = :hh5, description = "Household Category 5", set = :household),
            ])
        )


    HH = HouseholdTable(T, S, E; regularity_check=true)

    return HH

end



"""
    build_household_table(
            state_table::State,
            raw_data::RawHouseholdData;
        )


Build a `HouseholdTable` from the given `state_table` and `raw_data`.

## Disaggregation Steps

The disaggregation is performed through the following steps:

1. [`WiNDCHousehold.initialize_table`](@ref)
2. [`WiNDCHousehold.adjust_capital_demand`](@ref)
3. [`WiNDCHousehold.build_transfer_payments`](@ref)
"""
function build_household_table(
        state_table::State,
        raw_data::RawHouseholdData;
    )

    HH = initialize_table(state_table)
    HH = adjust_capital_demand(HH, state_table, raw_data)
    HH = build_transfer_payments(HH, state_table, raw_data)

    M1 = calibration_model_1(HH, state_table, raw_data)
    M2 = calibration_model_2(HH, state_table, raw_data, M1)

    HH = WiNDCHousehold.create_personal_consumption(HH, state_table, raw_data, M2)
    HH = WiNDCHousehold.create_labor_endowment(HH, state_table, raw_data, M1)
    HH = WiNDCHousehold.create_household_interest(HH, state_table, raw_data, M1)
    HH = WiNDCHousehold.update_household_transfers(HH, state_table, raw_data, M1)
    HH = WiNDCHousehold.create_taxes(HH, state_table, raw_data)

    return HH

end

"""
    adjust_capital_demand(
        HH::HouseholdTable,
        state_table::State,
        raw_data::RawHouseholdData;
    )

Convert gross capital demand in the `state_table` to net capital demand by removing
the capital tax based on state-level capital tax rates provided in `raw_data`.

```math
    {\\rm Net\\ Capital\\ Demand} = \\frac{\\rm Gross\\ Capital\\ Demand}{1 + r}
```

## Raw Data

- `raw_data.capital_tax_rates` (State-level capital tax rates)

## Parameters Disaggregated

- `Capital_Demand`

## Sets Added

- `capital_tax` 
    - Domain: `row`
    - Description: "Capital Tax"
    - Elements: `cap_tax`
- `Capital_Tax` - From `state_table`
"""
function adjust_capital_demand(
        HH::HouseholdTable,
        state_table::State,
        raw_data::RawHouseholdData;
    )

    capital_tax_rates = raw_data.capital_tax_rates

    capital_demand = table(state_table, :Capital_Demand) |>
        x -> outerjoin(x, capital_tax_rates, on = :region => :state) |>
        x -> transform(x, [:value, :capital_tax_rate] => ByRow((v, r) -> v / (1 + r)) => :value) |>
        x -> select(x, :row, :col, :region, :year, :parameter, :value)

    capital_tax = table(state_table, :Capital_Demand) |>
        x -> outerjoin(x, capital_tax_rates, on = :region => :state) |>
        x -> transform(x, [:value, :capital_tax_rate] => ByRow((v, r) -> v*r/(1+r)) => :value) |>
        x -> select(x, :row, :col, :region, :year, :parameter, :value) |>
        x -> transform(x, 
            :parameter => ByRow(p -> :capital_tax) => :parameter,
            :row => ByRow(r -> :cap_tax) => :row,
        )

    df = vcat(table(HH), capital_demand, capital_tax)

    S = vcat(
        sets(HH),
        sets(state_table, :Capital_Demand, :capital_demand),
        DataFrame([
            (name = :capital_tax, description = "Capital Tax", domain =:row),
            (name = :Capital_Tax, description = "Capital Tax", domain =:parameter),
        ])   
    )

    E = vcat(
        elements(HH),
        elements(state_table, :Capital_Demand, :capital_demand),
        DataFrame([
            (name = :cap_tax, description = "Capital Tax", set = :capital_tax),
            (name = :capital_tax, description = "Capital Tax", set = :Capital_Tax),
        ])
    )

    return HouseholdTable(df, S, E; regularity_check=true)
end

"""
    transfer_weights(raw_data::RawHouseholdData)

Compute transfer weights based on the CPS vs NIPA comparison in `raw_data`.

Transfer weight is defined as the ratio of NIPA transfer income to CPS transfer income
for each transfer category. If the CPS income for a category is zero, the weight
from either the Meyer or Rothbaum study is used as a fallback.

## Transfer Categories

| Category | Source |
|----------|--------|
|government benefits: unemployment insurance | hucval |
|government benefits: social security | hssval |
|government benefits: social security | hssival |
|government benefits: social security | hdisval |
|government benefits: veterans' benefits | hvetval |

## Meyer and Rothbaum Weights

| Source | Literature | Value |
|--------|--------------|-----------------|
| hucval | meyer |  1/0.679 | 
| hssval | meyer |  1/0.899 | 
| hssival | meyer |  1/0.759 | 
| hdisval | meyer |  1/0.819 | 
| hvetval | rothbaum |  1/0.679 | 
| hwcval | meyer |  1 / 0.527 | 
| hpawval | meyer |  1 / 0.487 | 
| hsurval | meyer |  1 / 0.908 | 
| hedval | rothbaum |  1 / 0.804 | 
| hcspval | rothbaum |  1 / 0.804 | 
| hfinval | meyer |  1 / 0.539 | 

!!! note ""
    The values are hard coded based on literature and may need to be updated as new studies emerge.
    Also, the year is fixed at 2024 for these weights. Need to update for more years in the future.
"""
function transfer_weights(raw_data::RawHouseholdData)
    cps_nipa = raw_data.nipa_cps

    return innerjoin(
        cps_nipa,
    
        DataFrame([
            (category = "government benefits: unemployment insurance", source = "hucval"),
            (category = "government benefits: social security", source = "hssval"),
            (category = "government benefits: social security", source = "hssival"),
            (category = "government benefits: social security", source = "hdisval"),
            (category = "government benefits: veterans' benefits", source = "hvetval"),
        ]),
        on = :category,
    ) |>
    x -> transform(x,
        [:nipa, :cps] => ByRow((n,c) -> n/c) => :value,
        :source => ByRow(y -> "nipa") => :variable
    ) |>
    x -> select(x, :year, :source, :variable, :value) |>
    x -> vcat(x,
    
        DataFrame([
            (year = 2024, source = "hucval", variable = "meyer", value = 1/0.679),
            (year = 2024, source = "hssval", variable = "meyer", value = 1/0.899),
            (year = 2024, source = "hssival", variable = "meyer", value = 1/0.759),
            (year = 2024, source = "hdisval", variable = "meyer", value = 1/0.819),
            (year = 2024, source = "hvetval", variable = "rothbaum", value = 1/0.679),
            (year = 2024, source = "hwcval", variable = "meyer", value = 1 / 0.527),
            (year = 2024, source = "hpawval", variable = "meyer", value = 1 / 0.487),
            (year = 2024, source = "hsurval", variable = "meyer", value = 1 / 0.908),
            (year = 2024, source = "hedval", variable = "rothbaum", value = 1 / 0.804),
            (year = 2024, source = "hcspval", variable = "rothbaum", value = 1 / 0.804),
            (year = 2024, source = "hfinval", variable = "meyer", value = 1 / 0.539),
        ])
    ) |>
    x -> unstack(x, :variable, :value) |>
    x -> transform(x,
        [:nipa, :meyer, :rothbaum] => ((n,m,r) -> coalesce.(n, coalesce.(m, r))) => :value
    ) |>
    x -> select(x, :year, :source, :value => :trn_weight)

end

"""
    build_transfer_payments(
        HH::HouseholdTable,
        raw_data::RawHouseholdData,
    )

Add transfer payments to the `HouseholdTable` `HH` based on the data in `raw_data`.

## Raw Data

- [`WiNDCHousehold.transfer_weights`](@ref)
- `raw_data.income` (CPS income data)
- `raw_data.medicare` (Medicare and Medicaid data)
- `raw_data.income_categories` (CPS income categories)

## Sets Added

- `Transfer_Payment` 
    - Domain: `parameter`
    - Description: "Transfer Payments"
    - Elements: `transfer_payment`

- `transfer_payments` 
    - Domain: `row`
    - Description: "Transfer Payments"
    
| Element | Description |
|---------|-------------|
| `hucval` | unemployment compensation |
| `hwcval` | workers compensation |
| `hssval` | social security |
| `hssival` |supplemental security |
| `hpawval` |public assistance or welfare |
| `hvetval` |veterans benefits |
| `hsurval` |survivors income |
| `hdisval` |disability |
| `hedval` | educational assistance |
| `hcspval` |child support |
| `hfinval` |financial assistance |
| `medicare` |edicare |
| `medicaid` |edicaid |
| `other` | Other Income | 

!!! note ""
    The year for medicare and medicaid is fixed at 2024. Need to update for more 
    years in the future.
"""
function build_transfer_payments(
        HH::HouseholdTable,
        state_table::State,
        raw_data::RawHouseholdData,
    )

    trn_weight = WiNDCHousehold.transfer_weights(raw_data)
    income_categories = raw_data.income_categories
    medicare = raw_data.medicare 
    cps = raw_data.income

    household_transfers = cps |>
            x -> innerjoin(x, income_categories, on = :source) |>
            x -> subset(x, :windc => ByRow(==("transfer"))) |>
            x -> outerjoin(
                x,
                trn_weight,
                on = [:year, :source],
            ) |>
            x -> transform(x,
                [:value, :trn_weight] => ByRow((v, w) -> v * w) => :value
            ) |>
            x -> select(x, :source => :row, :hh => :col, :state => :region, :year, :value) |>
            x -> vcat(
                x, 
                medicare |>
                    x -> subset(x, :year => ByRow(==(2024))) |>
                    x -> rename(x, 
                        :income => :col,
                        :variable => :row,
                        :state => :region,
                    )
            ) |>
            x -> transform(x,
                :row => ByRow(y -> :transfer_payment) => :parameter,
                [:row, :col] .=> ByRow(Symbol) .=> [:row, :col]
            )

    df = vcat(table(HH), household_transfers)
    S = sets(HH) |>
        x -> vcat(x,
            DataFrame([
                (name = :transfer_payment, description = "Transfer Payments", domain = :row),
                (name = :Transfer_Payment, description = "Transfer Payments", domain = :parameter),
            ])
        )
    E = elements(HH) |>
        x -> vcat(x,
            DataFrame([
                (name = :transfer_payment, description = "Transfer Payments", set = :Transfer_Payment),
                (name = :hucval,   set = :transfer_payment, description = "unemployment compensation"),
                (name = :hwcval,   set = :transfer_payment, description = "workers compensation"),
                (name = :hssval,   set = :transfer_payment, description = "social security"),
                (name = :hssival,  set = :transfer_payment, description = "supplemental security"),
                (name = :hpawval,  set = :transfer_payment, description = "public assistance or welfare"),
                (name = :hvetval,  set = :transfer_payment, description = "veterans benefits"),
                (name = :hsurval,  set = :transfer_payment, description = "survivors income"),
                (name = :hdisval,  set = :transfer_payment, description = "disability"),
                (name = :hedval,   set = :transfer_payment, description = "educational assistance"),
                (name = :hcspval,  set = :transfer_payment, description = "child support"),
                (name = :hfinval,  set = :transfer_payment, description = "financial assistance"),
                (name = :medicare, set = :transfer_payment, description = "medicare"),
                (name = :medicaid, set = :transfer_payment, description = "medicaid"),
                (name = :other,    set = :transfer_payment, description = "Other Income"),
            ])
        )

    return HouseholdTable(df, S, E; regularity_check=true)

end


"""
    compute_economy_wide_savings(
            state_table::WiNDCRegional.State
        )

Compute the economy-wide savings rate based on personal consumption and investment
from the `state_table`.

## Returns

- `economy_wide_savings::DataFrame`: A DataFrame with columns `:year` and `:economy_wide_sr0`
  representing the economy-wide savings rate for each year.

## Computation

```math
    {\\rm Economy\\ Wide\\ Savings\\ Rate} = \\frac{\\rm Investment\\ Final\\ Demand}{\\rm Personal\\ Consumption + Investment\\ Final\\ Demand}
```
"""
function compute_economy_wide_savings(
        state_table::WiNDCRegional.State
    )

    economy_wide_savings = table(state_table, :Personal_Consumption, :Investment_Final_Demand) |>
        x -> select(x, Not(:col)) |>
        x -> unstack(x, :parameter, :value) |>
        x -> coalesce.(x, 0) |>
        x -> groupby(x, :year) |>
        x -> combine(x, 
            [:personal_consumption, :investment_final_demand] .=> sum .=> [:personal_consumption, :investment_final_demand]
        ) |>
        x -> transform(x,
            [:personal_consumption, :investment_final_demand] => ByRow((c,i) -> i/(c+i)) => :economy_savings
        ) |>
        x -> select(x, :year, :economy_savings)

    return economy_wide_savings
end


"""
    get_capital_ownership(
        raw_data::RawHouseholdData;
        type::Symbol = :all
    )

Get the capital ownership share from `raw_data`.

## Arguments

- `raw_data::RawHouseholdData`: The raw household data containing CPS and NIPA data.

## Keyword Arguments

- `type::Symbol = :all`: The type of capital ownership to retrieve. Options are:
    - `:all`: Returns 1 (100% ownership).
    - `:partial`: Computes the ownership share based on CPS vs NIPA data.

## Returns

- `cap_own::Float64`: The capital ownership share.
"""
function get_capital_ownership(
        raw_data::RawHouseholdData;
        type::Symbol = :all
    )

    cap_own = 1
    if type == :partial
        raw_data.windc_vs_nipa_income_categories |>
            x -> subset(x, :parameter => ByRow(==(:capital_ownership))) |>
            x -> transform(x, 
                [:nipa, :windc] => ByRow((n, w) -> n / w) => :cap_own
            ) |>
            x -> cap_own = x[1, :cap_own]
    end

    return cap_own
end

"""
    savings_markup_save(
        HH::HouseholdTable,
        raw_data::RawHouseholdData
    )

Compute the savings markup for the household table `HH` based on the raw CPS data
in `raw_data`.

## Arguments

- `HH::HouseholdTable`: The household table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `savings_markup_save::Float64`: The computed savings markup.

## Raw Data Used

- `raw_data.cps_data` (CPS interest data)
- [`WiNDCHousehold.get_capital_ownership`](@ref)

## Computation

```math
    {\\rm Savings\\ Markup} = \\frac{\\rm  Gross\\ Capital\\ Demand}{\\rm Total\\ Interest}
```
"""
function compute_savings_markup_save(HH::HouseholdTable, raw_data::RawHouseholdData)

    cap_own = get_capital_ownership(raw_data, type = :all)

    interest = raw_data.cps_data |>
        x -> unstack(x, :windc, :value) |>
        x -> select(x, :hh, :year, :state, :interest)

    i = interest |>
        x -> combine(x, :interest => sum => :interest) |>
        x -> x[1,1]

    k = table(HH, :Capital_Demand; normalize = :Use) |>
        x -> combine(x, :value => (y -> cap_own * sum(y)) => :value) |>
        x -> x[1,1]

    savings_markup_save = k/i

    return savings_markup_save
end


"""
    compute_savings_markup_consumption(
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

Compute the savings markup based on personal consumption from the `state_table`
and CPS data in `raw_data`.

## Arguments

- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `savings_markup_consumption::Float64`: The computed savings markup based on consumption.

## Computation

```math
    {\\rm Savings\\ Markup} = \\frac{\\rm Total\\  Personal\\ Consumption}{\\rm Total\\ Consumption}
```
"""
function compute_savings_markup_consumption(state_table::WiNDCRegional.State, raw_data::RawHouseholdData)
    consumption = raw_data.cps_data |>
        x -> combine(x, :value => sum => :value) |>
        x -> x[1,1]

    pc = table(state_table, :Personal_Consumption; normalize = :Use) |>
        x -> combine(x, :value => sum => :value) |>
        x -> x[1,1]

    return pc/consumption
end


"""
    compute_savings_rate(
            HH::HouseholdTable,
            state_table::WiNDCRegional.State,
            raw_data::RawHouseholdData
        )


Compute the savings rate for each household in `HH` based on the economy-wide
savings rate and the household's interest income and consumption.

## Arguments

- `HH::HouseholdTable`: The household table.
- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `savings_rate::DataFrame`: A DataFrame with columns `:hh`, `:year`, `:state`, and `:save_rate`
  representing the savings rate for each household.

## Computation

This computation is broken into several steps:

1. Compute the economy-wide savings rate using [`WiNDCHousehold.compute_economy_wide_savings`](@ref).
2. Compute the savings markup for savings and consumption using:
   - [`WiNDCHousehold.compute_savings_markup_save`](@ref) and 
   - [`WiNDCHousehold.compute_savings_markup_consumption`](@ref).
3. Calculate the interest shares and total shares for each household.
4. Finally, compute the savings rate for each household using the formula:

```math
    {\\rm Savings\\ Rate}_{hh} = \\frac{\\rm (Economy\\ Wide\\ Savings\\ Rate) \\cdot (Interest\\ Share)}{Total\\ Share}}
```
"""
function compute_savings_rate(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

    economy_wide_savings = WiNDCHousehold.compute_economy_wide_savings(state_table)
    savings_markup_save = WiNDCHousehold.compute_savings_markup_save(HH, raw_data)
    savings_markup_consumption = WiNDCHousehold.compute_savings_markup_consumption(state_table, raw_data)

    interest = raw_data.cps_data |>
        x -> unstack(x, :windc, :value) |>
        x -> select(x, :hh, :year, :state, :interest)

    consumption = raw_data.cps_data |>
        x -> groupby(x, [:hh, :year, :state]) |>
        x -> combine(x, :value => sum => :consumption)


    interest_shares = interest |>
        x -> groupby(x, [:year]) |>
        x -> combine(x,
            [:hh, :state] .=> identity .=> [:hh, :state],
            :interest => (y -> y./sum(y)) => :interst_share
        )

    total_shares = outerjoin(interest, consumption, on = [:hh, :year, :state]) |>
        x -> transform(x, 
            [:interest, :consumption] => ByRow((i,c) -> savings_markup_save*i + savings_markup_consumption*c) => :value
        ) |>
        x -> groupby(x, [:year]) |>
        x -> combine(x,
            [:hh, :state] .=> identity .=> [:hh, :state],
            :value => (y -> y./sum(y)) => :total_share
        )

    savings_rate = outerjoin(interest_shares, total_shares, on = [:hh, :state, :year]) |>
        x -> outerjoin(x, economy_wide_savings, on = :year) |>
        x -> transform(x,
            [:economy_savings, :interst_share, :total_share] => 
                ByRow((es, is, ts) -> ts == 0 ? 0.0 : es*is/ts) => :save_rate
        ) |>
        x -> select(x, :hh, :year, :state, :save_rate) 

    return savings_rate
        
end



"""
    adjusted_consumption(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

Compute the adjusted consumption for each household in `HH` based on consumption shares,
personal consumption from `state_table`, and the household savings rate.

## Arguments

- `HH::HouseholdTable`: The household table.
- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `adjusted_consumption::DataFrame`: A DataFrame with columns `:hh`, `:year`, `:state`, and `:consumption`
  representing the adjusted consumption for each household.

## Computation

The adjusted consumption is computed using the formula:

```math
    {\\rm Adjusted\\ Consumption} = \\frac{{\\rm Consumption\\ Share} \\cdot {\\rm Personal\\ Consumption}}{1 - {\\rm Savings\\ Rate}}
```
"""
function adjusted_consumption(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

    savings_rate = WiNDCHousehold.compute_savings_rate(
        HH,
        state_table,
        raw_data;
    )

    consumption_shares = raw_data.cps_data |>
        x -> groupby(x, [:hh, :year, :state]) |>
        x -> combine(x, :value => sum => :value) |>
        x -> groupby(x, [:year, :state]) |>
        x -> combine(x,
            :hh => identity => :hh,
            :value => (y -> y./sum(y)) => :consumption_share
        )

    c0 = table(state_table, :Personal_Consumption; normalize=:Use) |>
        x -> groupby(x, [:year, :region]) |>
        x -> combine(x, :value => sum => :pce)

    adjusted_consumption = outerjoin(
            consumption_shares,
            savings_rate,
            on = [:hh, :year, :state]
        ) |>
        x -> innerjoin(
            x,
            c0,
            on = [:year => :year, :state => :region]
        ) |>
        x -> transform(x,
            [:consumption_share, :pce, :save_rate] => ByRow((cons, pce, sr) -> cons*pce/(1-sr) ) => :consumption
        ) |>
        x -> select(x, :hh, :year, :state, :consumption) 

    return adjusted_consumption

end





"""
    labor_endowment_multiplier(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

Compute the labor endowment multiplier for each region based on labor demand and wages.

## Arguments

- `HH::HouseholdTable`: The household table.
- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `labor_endow_multiplier::DataFrame`: A DataFrame with columns `:region` and `:le_mult`
  representing the labor endowment multiplier for each region.

## Computation

The labor endowment multiplier is computed using the formula:

```math
    {\\rm Labor\\ Endowment\\ Multiplier} = \\frac{\\rm Fringe\\ Benefit\\ Markup \\cdot \\rm Wage}{\\rm Labor\\ Demand}
```

Changes also made based on commuting patterns to ensure that regions without
commuting workers do not have their labor endowment multipliers adjusted
beyond 1.
"""
function labor_endowment_multiplier(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

    function reset_labor_by_commute(region, fringe, labor, wage, commute_origin, commute_destination)
        le = fringe * wage / labor
        le = (le > 1 && region ∉ commute_origin) ? 1 : le
        le = (le < 1 && region ∉ commute_destination) ? 1 : le
        return le
    end


    labor_demand = table(HH, :Labor_Demand; normalize=:Use) 

    wages = raw_data.cps_data |>
        x -> subset(x, :windc => ByRow(y -> y == "wages")) |>
        x -> select(x, :hh, :year, :state, :value => :wages) 

    total_labor = labor_demand |>
        x -> combine(x, :value => sum => :value) |>
        x -> x[1,1]

    total_wage = wages |>
        x -> combine(x, :wages => sum => :wages) |>
        x -> x[1,1]


    fringe_markup = total_labor / total_wage
    
    
    commute = raw_data.acs_commute
    
    commute_origin = commute |>
        x -> x[!, :home_state] |>
        unique
    
    commute_destination = commute |>
        x -> x[!, :work_state] |>
        unique
    
    labor_endow_multiplier = outerjoin(
            labor_demand |>
                x -> groupby(x, :region) |>
                x -> combine(x, :value => sum => :labor),
            
            wages |>
                x -> groupby(x, :state) |>
                x -> combine(x, :wages => sum => :wages),

            on = [:region => :state]
        ) |>
        x -> transform(x,
            [:region, :labor, :wages] => ByRow((r, l, w) -> reset_labor_by_commute(r, fringe_markup, l, w, commute_origin, commute_destination) ) => :le_mult
        ) |>
        x -> select(x, :region, :le_mult)
    
    return labor_endow_multiplier

end

"""
    adjusted_wages(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData;
        tax_adjustment::Bool = true
    )

Compute the adjusted wages for each household in `HH` based on labor endowment multipliers
and labor tax rates.

## Arguments

- `HH::HouseholdTable`: The household table.
- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data and labor tax rates.

## Keyword Arguments

- `tax_adjustment::Bool = true`: Whether to apply labor tax adjustments.

## Returns

- `adjusted_wages::DataFrame`: A DataFrame with columns `:hh`, `:year`, `:state`, and `:wage`
  representing the adjusted wages for each household.

## Computation

The adjusted wages are computed using the formula:

```math
    {\\rm Adjusted\\ Wage} = {\\rm Wage\\ Share} \\cdot ({\\rm tax\\ adjustment}\\cdot {\\rm Labor\\ Tax\\ Rate} - 1) \\cdot {\\rm Labor\\ Endowment\\ Multiplier} \\cdot \\sum_{sectors}{\\rm Labor\\ Demand}
```
"""
function adjusted_wages(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData;
        tax_adjustment::Bool = true
    )

    labor_endow_multiplier = WiNDCHousehold.labor_endowment_multiplier(
        HH,
        state_table,
        raw_data;
    )

    wages = raw_data.cps_data |>
        x -> subset(x, :windc => ByRow(y -> y == "wages")) |>
        x -> select(x, :hh, :year, :state, :value => :wages) 

    labor_demand = table(HH, :Labor_Demand; normalize=:Use) 
 
    adjusted_wage = innerjoin(
            wages |>
                x -> groupby(x, [:year, :state]) |>
                x -> combine(x, 
                    :hh => identity => :hh,
                    :wages => (y -> y/sum(y)) => :wage_share
                ),
            raw_data.labor_tax_rates |>
                x -> subset(x, :variable => ByRow(∈(["tfica","tl_avg"]))) |>
                x -> groupby(x, [:hh, :state]) |>
                x -> combine(x, :labor_tax_rate => (y -> tax_adjustment ? sum(y) - 1 : 1) => :tax_rate),
            on = [:hh, :state]
        ) |>
        x -> innerjoin(
            x, 
            labor_endow_multiplier,

            labor_demand |>
                x -> groupby(x, [:region]) |>
                x -> combine(x, :value => sum => :labor),

            on = [:state => :region]
        ) |>
        x -> transform(x,
            [:wage_share, :tax_rate, :le_mult, :labor] => ByRow(*) => :wage
        ) |>
        x -> select(x, :hh, :year, :state, :wage)

    return adjusted_wage
end



"""
    adjusted_capital_income(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

Compute the adjusted capital income for each household in `HH` based on capital ownership
and total capital demand.

## Arguments

- `HH::HouseholdTable`: The household table.
- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `adjusted_capital::DataFrame`: A DataFrame with columns `:hh`, `:year`, `:state`, and `:capital`
  representing the adjusted capital income for each household.

## Computation

The adjusted capital income is computed using the formula:

```math
    {\\rm Adjusted\\ Capital\\ Income} = {\\rm Capital\\ Ownership\\ Share} \\cdot {\\rm Total\\ Capital\\ Demand} \\cdot \\frac{{\\rm Interest\\ Income}}{\\sum_{hh}{\\rm Interest\\ Income}}
```
"""
function adjusted_capital_income(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )


    cap_own = get_capital_ownership(raw_data; type = :all)

    total_cap = table(HH, :Capital_Demand; normalize=:Use) |>
        x -> groupby(x, :year) |>
        x -> combine(x, :value => sum => :capital) |>
        x -> x[1, :capital]


    adjusted_capital = raw_data.cps_data |>
        x -> subset(x, :windc => ByRow(y -> y == "interest")) |>
        x -> transform(x,
            :value => (y -> cap_own*total_cap * y/sum(y)) => :capital
        ) |>
        x -> select(x, :hh, :year, :state, :capital)

    return adjusted_capital

end




"""
    other_income(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

Compute the other income for each household in `HH` based on adjusted consumption,
adjusted wages, transfer payments, and adjusted capital income.

## Arguments

- `HH::HouseholdTable`: The household table.
- `state_table::WiNDCRegional.State`: The state-level WiNDC table.
- `raw_data::RawHouseholdData`: The raw household data containing CPS data.

## Returns

- `other_income::DataFrame`: A DataFrame with columns `:hh`, `:year`, `:state`, and `:income`
  representing the other income for each household.

## Computation

The other income is computed using the formula:

```math
    {\\rm Other\\ Income} = {\\rm Adjusted\\ Consumption} + {\\rm Adjusted\\ Wages} - {\\rm Average\\ Transfer\\ Payments} - {\\rm Adjusted\\ Capital\\ Income}
```

where:

- `Adjusted Consumption` is obtained from [`WiNDCHousehold.adjusted_consumption`](@ref).
- `Adjusted Wages` is obtained from [`WiNDCHousehold.adjusted_wages`](@ref).
- `Average Transfer Payments` the average over households and states of the `Transfer_Payment` parameter in `HH`.
- `Adjusted Capital Income` is obtained from [`WiNDCHousehold.adjusted_capital_income`](@ref).
"""
function other_income(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

    term1 = WiNDCHousehold.adjusted_consumption(HH, state_table, raw_data)
    term2 = WiNDCHousehold.adjusted_wages(HH, state_table, raw_data)

    term3 = table(HH, :Transfer_Payment) |>
        x -> groupby(x, [:region, :year, :col]) |>
        x -> combine(x, :value => sum => :adj_trans) |>
        x -> transform(x, :col => ByRow(String) => :col) |>
        x -> select(x, :col => :hh, :region => :state, :year, :adj_trans)

    term4 = WiNDCHousehold.adjusted_capital_income(HH, state_table, raw_data)

    other_income = outerjoin(
        term1,    
        term2,
        term3,
        term4,
        on = [:hh, :year, :state]
    ) |>
    x -> coalesce.(x, 0) |>
    x -> transform(x,
        [:consumption, :wage, :adj_trans, :capital] => ByRow(
            (c, w, t, k) -> c + w - t - k
        ) => :income
    ) |>
    x -> select(x, :hh, :year, :state, :income)

    return other_income

end




"""
    create_personal_consumption(
            HH::HouseholdTable,
            state_table::WiNDCRegional.State,
            raw_data::RawHouseholdData,
            M2::JuMP.Model
        )


"""
function create_personal_consumption(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData,
        M2::JuMP.Model
    )

    regions = elements(HH, :state) |> x -> x[:, :name]
    households = elements(HH, :household) |> x -> x[:, :name]
    commodities = elements(HH, :commodity) |> x -> x[:, :name]

    personal_consumption = DataFrame(vec([
        (region = r, row = g, col = h, year = 2024, parameter = :personal_consumption, value = -value(M2[:CD][r, g, h]))
        for r in regions, g in commodities, h in households
    ]))


    df = table(HH) |>
        x -> subset(x, :parameter => ByRow(!=(:personal_consumption))) |>
        x -> vcat(x, personal_consumption) 

    S = sets(HH) |>
        x -> subset(x, :name => ByRow(∉([:Personal_Consumption]))) |>
        x -> vcat(x,
            DataFrame([
                (name = :Personal_Consumption, description = "Personal Consumption", domain = :parameter),
            ])
        )

    E = elements(HH) |>
        x -> subset(x, :name => ByRow(∉([:personal_consumption]))) |>
        x -> vcat(x,
            DataFrame(vec([
                (name = :personal_consumption, set = :Personal_Consumption, description = "Personal Consumption"),
                (name = :personal_consumption, set = :Other_Final_Demand, description = "Personal Consumption"),
                (name = :personal_consumption, set = :Final_Demand, description = "Personal Consumption"),
                (name = :personal_consumption, set = :Use, description = "Personal Consumption"),
            ]))
        )

    HH = HouseholdTable(df, S, E)

    return HH
end



function create_labor_endowment(
        HH::HouseholdTable, 
        state_table::State, 
        HH_Raw_Data::RawHouseholdData, 
        M1::JuMP.Model
    )

    regions = elements(HH, :state) |> x -> x[:, :name]
    households = elements(HH, :household) |> x -> x[:, :name]
    
    labor_endowment = DataFrame(vec([
        (region = origin, col = h, row = destination, year = 2024, parameter = :labor_endowment, value = value(M1[:Wages][origin, destination, h]))
        for origin in regions, destination in regions, h in households if value(M1[:Wages][origin, destination, h]) != 0
    ]))
    
    df = table(HH) |>
        x -> vcat(x, labor_endowment)
    
    S = sets(HH) |>
        x -> vcat(x,
            DataFrame([
                (name = :Labor_Endowment, description = "Labor Endowment", domain = :parameter),
                (name = :destination, description = "Destination Region", domain = :row),
            ])
        )
    
    E = elements(HH) |>
        x -> vcat(x,
            DataFrame(vec([
                (name = :labor_endowment, set = :Labor_Endowment, description = "Labor Endowment"),
                [(name = r, set = :destination, description = "Destination $(r)") for r in regions]...
            ]))
        )
    
    
    HH = HouseholdTable(df, S, E; regularity_check = true)
    return HH

end




function create_household_interest(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData,
        M1::JuMP.Model
    )

    regions = elements(HH, :state) |> x -> x[:, :name]
    households = elements(HH, :household) |> x -> x[:, :name]

    household_interest = DataFrame(vec([
        (region = r, col = h, row = :interest, year = 2024, parameter = :household_interest, value = value(M1[:Interest][r, h]))
        for r in regions, h in households if value(M1[:Interest][r, h]) != 0
    ]))

    df = table(HH) |>
        x -> vcat(x, household_interest)

    S = sets(HH) |>
        x -> vcat(x,
            DataFrame([
                (name = :Household_Interest, description = "Household Interest", domain = :parameter),
                (name = :interest, description = "Interest", domain = :row),
            ])
        )

    E = elements(HH) |>
        x -> vcat(x,
            DataFrame(vec([
                (name = :household_interest, set = :Household_Interest, description = "Household Interest"),
                (name = :interest, set = :interest, description = "Interest"),
            ]))
        )


    HH = HouseholdTable(df, S, E; regularity_check = true)

    return HH
end


function update_household_transfers(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData,
        M1::JuMP.Model
    )

    regions = elements(HH, :state) |> x -> x[:, :name]
    households = elements(HH, :household) |> x -> x[:, :name]

    new_transfers = table(HH, :Transfer_Payment) |>
        x -> groupby(x, [:region, :col, :year]) |>
        x -> combine(x,
            [:row, :parameter] .=> identity .=> [:row, :parameter],
            :value => (y -> y/sum(y)) => :value
        ) |>
        x -> transform(x,
            [:region, :col, :value] => ByRow((r,c,v) -> v*value(M1[:Government_Transfers][r, c])) => :value
        ) |>
        x -> vcat(x,
            DataFrame(vec([
                (row = :other, region = r, col = hh, parameter = :transfer_payment, year = 2024, value = value(M1[:Other_Income][r, hh]))
                for r in regions, hh in households
            ]))
        ) |>
        x -> subset(x, :value => ByRow(!=(0)))


    df = table(HH) |>
        x -> subset(x, :parameter => ByRow(!=( :transfer_payment ))) |>
        x -> vcat(x, new_transfers)

    HH = HouseholdTable(df, sets(HH), elements(HH); regularity_check = true)
    return HH

end



function create_taxes(
        HH::HouseholdTable,
        state_table::WiNDCRegional.State,
        raw_data::RawHouseholdData
    )

        
    tax_encoding = DataFrame([
        (row = :mlt, variable = "tl", parameter = :marginal_labor_tax),
        (row = :fica, variable = "tfica", parameter = :fica_tax),
        (row = :tla, variable = "tl_avg", parameter = :average_labor_tax),
    ])


    taxes = innerjoin(
        table(HH, :Labor_Endowment) |>
            x -> groupby(x, [:region, :col, :year]) |>
            x -> combine(x,
                :value => sum => :labor
            ),
        
        raw_data.labor_tax_rates |>
            x -> innerjoin(x, tax_encoding, on = :variable) |>
            x -> transform(x, :hh => ByRow(Symbol) => :col),
        on = [:region => :state, :col]
    ) |>
    x -> transform(x,
        [:labor, :labor_tax_rate] => ByRow((l, t) -> l * t) => :value
    ) |>
    x -> select(x, :row, :col, :year, :region, :parameter, :value)


    df = table(HH) |>
        x -> subset(x, :parameter => ByRow(!in([:marginal_labor_tax, :fica_tax, :average_labor_tax]))) |>
        x -> vcat(x, taxes)

    S = sets(HH) |>
        x -> vcat(x,
            DataFrame([
                (name = :Marginal_Labor_Tax, description = "Marginal Labor Tax", domain = :parameter),
                (name = :FICA_Tax, description = "FICA Tax", domain = :parameter),
                (name = :Average_Labor_Tax, description = "Average Labor Tax", domain = :parameter),
                (name = :marginal_labor_tax, description = "Marginal Labor Tax", domain = :row),
                (name = :fica, description = "FICA Tax", domain = :row),
                (name = :average_labor_tax, description = "Average Labor Tax", domain = :row),
            ])
        )

    E = elements(HH) |>
        x -> vcat(x,
            DataFrame(vec([
                (name = :marginal_labor_tax, set = :Marginal_Labor_Tax, description = "Marginal Labor Tax"),
                (name = :fica_tax, set = :FICA_Tax, description = "FICA Tax"),
                (name = :average_labor_tax, set = :Average_Labor_Tax, description = "Average Labor Tax"),
                (name = :mlt, set = :marginal_labor_tax, description = "Marginal Labor Tax"),
                (name = :fica, set = :fica, description = "FICA Tax"),
                (name = :tla, set = :average_labor_tax, description = "Average Labor Tax"),
            ]))
        )

    HH = HouseholdTable(df, S, E; regularity_check = true)

    return HH

end