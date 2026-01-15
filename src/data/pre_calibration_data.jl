"""
    transfer_weights(raw_data::RawHouseholdData)

Compute transfer weights based on the CPS vs NIPA comparison in `raw_data`.

Transfer weight is defined as the ratio of NIPA transfer income to CPS transfer income
for each transfer category. If the CPS income for a category is zero, the weight
from either the Meyer or Rothbaum study is used as a fallback.

Returns a DataFrame with the following columns:

- `year`: The year of the data.
- `source`: The CPS income source/category.
- `trn_weight`: The computed transfer weight for the category.

## Arguments

- `raw_data::RawHouseholdData`: The raw household data containing CPS and NIPA income data.

## Raw Input Data

- `nipa_cps`: A DataFrame containing NIPA and CPS income data by category.

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

## Process

1. Extract the transfer categories from the `cps_nipa`
2. Compute the transfer weight as the ratio of NIPA to CPS income.
3. Add Meyer and Rothbaum weights for categories where CPS income is zero, 
    preference given to Meyer weights.

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
    initial_transfer_payments(
        HH::HouseholdTable,
        state_table::State,
        raw_data::RawHouseholdData,
    )

Generate initial transfer payments for households by applying transfer weights
to CPS income and adding Medicare and Medicaid data.

Returns a DataFrame with the following columns:

- `row`: The transfer payment category.
- `col`: The household identifier.
- `region`: The state identifier.
- `year`: The year of the data.
- `parameter`: The parameter name, set to `:transfer_payment`.
- `value`: The computed transfer payment value for the household.

## Arguments

- `HH::HouseholdTable`: The household table containing household information.
- `state_table::State`: The state table containing state information.
- `raw_data::RawHouseholdData`: The raw household data containing CPS income,
  Medicare, Medicaid, and income categories.

The first two arguments are included for consistency with other initial data functions,
but are not directly used in this function.


## Raw Input Data

- [`WiNDCHousehold.transfer_weights`](@ref)
- `raw_data.income` (CPS income data)
- `raw_data.medicare` (Medicare and Medicaid data)
- `raw_data.income_categories` (CPS income categories)

## Transfer Payment Categories

    
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

## Process

1. Subset the CPS income data to include only transfer payment categories.
2. Join with transfer weights and multiply CPS income by the corresponding weight.
3. Add Medicare and Medicaid data for the year 2024.

!!! note "Data fixed at 2024"
    The year for medicare and medicaid is fixed at 2024. Need to update for more 
    years in the future.
"""
function initial_transfer_payments(
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
            x -> subset(x, :windc => ByRow(==("transfer"))) |> # Only used to get the transfer categories. Could we do better?
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

    return household_transfers

end