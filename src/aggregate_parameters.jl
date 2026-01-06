"""
    labor_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :labor_supply,
    )

Calculate the labor supply for households. The labor supply is defined as the labor endowment
adjusted for marginal labor taxes and FICA taxes.

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:labor_supply`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :ls | household | state | year | :labor_supply | value |

Non-symbol values are sets.

## Parameters 

These values are extracted from the HouseholdTable

- `Labor Endowment`
- `Marginal Labor Tax`
- `FICA Tax`

## Calculation

```math
{\\rm Labor\\_Supply} = \\sum (Labor\\_Endowment - Marginal\\_Labor\\_Tax - FICA\\_Tax)
```
"""
function labor_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :labor_supply,
    )

    return table(HH, :Labor_Endowment, :Marginal_Labor_Tax, :FICA_Tax) |>
        x -> transform(x, 
            [:parameter, :value] => ByRow((p,v) -> p∈[:marginal_labor_tax, :fica_tax] ? -v : v) => :value
        ) |>
        x -> groupby(x, [:region, :year, :col]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, 
            :col => ByRow(y -> (:ls, parameter)) => [:row, :parameter]
        )

end


"""
    total_supply(
        data::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :total_supply
    )

Calculate the total supply for each commodity. 
    
## Arguments

- `data::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol = :value`: The column to aggregate (default is `:value`).
- `output::Symbol = :value`: The name of the output column (default is `:value`).
- `parameter::Symbol = :total_supply`: The parameter to assign to the output (default is `:total_supply`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :tot_sup | state | year | :total_supply | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Intermediate_Supply`
- `Household_Supply`

## Calculation
    
```math
{\\rm Total\\_Supply} = \\sum_{sectors} {\\rm Intermediate\\_Supply}} + {{\\rm Household\\_Supply}}
```
"""
function total_supply(
        data::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :total_supply
    )
    
    return table(data, :Intermediate_Supply, :Household_Supply) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, :row => ByRow(y -> (:tot_sup, parameter)) => [:col, :parameter])
end

"""
    absorption(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :absorption,
        normalize::Bool = false
    )

Calculate the absorption for each commodity in each region. 

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:absorption`).
- `normalize::Bool`: Flip the resulting sign of the data (default is `false`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :abs | state | year | :absorption | value |

Non-symbol values are sets.

!!!note "Negative Values
    Absorption is a demand-side value, which means the resulting values will be 
    negative. If you wish to have positive values, set the `normalize` argument to `true`.

## Parameters

These values are extracted from the HouseholdTable

- `Intermediate_Demand`
- `Other_Final_Demand`

## Calculation

```math
{\\rm Absorption} = \\sum_{\\rm sectors} {\\rm Intermediate\\_Demand}} + \\sum_{\\rm other\\_final\\_demand}{{\\rm Other\\_Final\\_Demand}}
```
"""
function absorption(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter::Symbol = :absorption,
        normalize::Bool = false
    )
    return table(HH, :Intermediate_Demand, :Other_Final_Demand) |>
        x -> groupby(x, [:row, :year, :region]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, 
            :value => ByRow(y -> normalize ? -y : y) => :value,
            :row => ByRow(y -> (:abs, parameter)) => [:col, :parameter]
        )
end

"""
    regional_local_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_local_supply,
    )

Calculate the regional local supply for each commodity in each region. 
    
## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:region_local_supply`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :rls | state | year | :region_local_supply | value |

Non-symbol values are sets.
    
## Parameters

These values are extracted from the HouseholdTable

- `Local_Margin_Supply`
- `Local_Demand`

## Calculation

```math
{\\rm Regional\\_Local\\_Supply} = \\sum (Local\\_Margin\\_Supply + Local\\_Demand)
```
"""
function regional_local_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_local_supply,
    )

    df = table(HH, :Local_Margin_Supply, :Local_Demand; normalize=:Use) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :row => ByRow(y -> (:rls, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])


    return df
end

"""
    netports(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :netport
    )

Calculate the netports for each commodity in each region. 

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:netport`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :netport | state | year | :netport | value |

Non-symbol values are sets.

## Parameters 

These values are extracted from the HouseholdTable 

- `Export`
- `Reexport`

## Calculation

```math
{\\rm Netports} = \\sum (Export + Reexport)
```
"""
function netports(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :netport
    )

    df = table(HH, :Export, :Reexport; normalize = :Export) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :row => ByRow(y -> (:netport, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df
end


"""
    regional_national_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_national_supply
    )

Calculate the regional national supply for each commodity in each region. 

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:region_national_supply`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :rns | state | year | :region_national_supply | value |

Non-symbol values are sets.

## Aggregate Parameters

- [`WiNDCHousehold.total_supply`](@ref)
- [`WiNDCHousehold.netports`](@ref)
- [`WiNDCHousehold.regional_local_supply`](@ref)

## Calculation

```math
{\\rm Regional\\_National\\_Supply} = {\\rm Total\\_Supply} - {\\rm Netports} - {\\rm Regional\\_Local\\_Supply}
```


"""
function regional_national_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_national_supply
    )

    df = outerjoin(
        total_supply(HH; column = column, output = :total_supply) |> x -> select(x, :row, :region, :year, :total_supply),
        netports(HH; column = column, output = :netport) |> x -> select(x, :row, :region, :year, :netport),
        regional_local_supply(HH; column = column, output = :rls) |> x -> select(x, :row, :region, :year, :rls),
        on = [:row, :region, :year]
        ) |>
    x -> coalesce.(x, 0) |>
    x -> transform(x,
        [:total_supply, :netport, :rls] => ByRow((ts, np, rls) -> ts - np - rls) => output,
        :row => ByRow(y -> (:rns, parameter)) => [:col, :parameter]
    ) |>
    x -> select!(x, [:row, :col, :region, :year, :parameter, output])

    return df

end



"""
    output_tax_rate(
            HH::HouseholdTable; 
            column::Symbol = :value, 
            output::Symbol = :value,
            parameter = :output_tax_rate
        )

Calculate the output tax rate for each commodity in each region.

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:output_tax_rate`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :otr | sector | state | year | :output_tax_rate | value |

Non-symbol values are sets.

## Parameters 

- `Intermediate_Supply`
- `Output_Tax`

## Calculation

```math
{\\rm Output\\_Tax\\_Rate} = \\frac{\\rm Output\\_Tax}{\\sum_{\\rm commodity} \\rm Intermediate\\_Supply}
```
"""
function output_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :output_tax_rate
    )

    df = innerjoin(
        table(HH, :Intermediate_Supply) |> 
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :is),
        table(HH, :Output_Tax; column = column, normalize= :Use),
        on = [:col, :region, :year]
        ) |>
        x -> transform(x,
            [:is, column] => ByRow((is, ot) -> ot / is) => output,
            :col => ByRow(y -> (:otr, parameter)) => [:row, :parameter]
        )  |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df

end

"""
    tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :tax_rate
    )

Calculate the tax rate, inclusive of subsidies, for each commodity in each region. 

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:tax_rate`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :tr | state | year | :tax_rate | value |

Non-symbol values are sets.

## Parameters 

These values are extracted from the HouseholdTable

- `Tax`

## Aggregate Parameters

- [`WiNDCHousehold.absorption`](@ref)

## Calculation

```math
{\\rm Tax\\_Rate} = \\frac{\\rm Tax}{\\rm Absorption}
```
"""
function tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :tax_rate
    )

    df = innerjoin(
        table(HH, :Tax),
        absorption(HH; column=column, normalize=true) |> x-> select(x, :row, :year, :region, :value => :absorption),
        on = [:row, :region, :year]
        ) |>
        x -> transform(x,
            [column, :absorption] => ByRow((tx, ab) -> tx / ab) => output,
            :row => ByRow(y -> (:tr, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df

end



"""  
  duty_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :duty_rate
    )
   
Calculate the duty rate for each commodity in each region. 
    
## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:duty_rate`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| commodity | :dr | state | year | :duty_rate | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Duty`
- `Import`

## Calculation

```math
{\\rm Duty\\_Rate} = \\frac{\\rm Duty}{\\rm Import}
```
"""
function duty_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :duty_rate,
        minimal::Bool = false
    )

    df = innerjoin(
        table(HH, :Duty),
        table(HH, :Import) |> x-> select(x, :row, :year, :region, :value => :import),
        on = [:row, :region, :year]
        ) |>
        x -> transform(x,
            [column, :import] => ByRow((dy, ts) -> dy / ts) => output,
            :row => ByRow(y -> (:dr, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df

end

"""
    marginal_labor_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :marginal_labor_tax_rate
    )

Calculate the marginal labor tax rate for each household type in each region. 

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:marginal_labor_tax_rate`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :ltr | household | state | year | :marginal_labor_tax_rate | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Marginal_Labor_Tax`
- `Labor_Endowment`

## Calculation

The marginal labor tax rate is calculated as:

```math
{\\rm Marginal\\_Labor\\_Tax\\_Rate} = \\frac{\\rm Marginal\\_Labor\\_Tax}{\\sum_{\\rm Destinations} \\rm Labor\\_Endowment}
```
"""
function marginal_labor_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :marginal_labor_tax_rate,
        minimal::Bool = false
    )

    df = innerjoin(
        table(HH, :Labor_Endowment) |> 
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :le),
        table(HH, :Marginal_Labor_Tax; column = column, normalize= :Use),
        on = [:col, :region, :year]
        ) |>
        x -> transform(x,
            [:le, column] => ByRow((le, ot) -> ot / le) => output,
            :col => ByRow(y -> (:ltr, parameter)) => [:row, :parameter]
        )  |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:col, :region, :year, :parameter, output])
    end

    return df

end

"""
    fica_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :fica_tax_rate,
    )

Calculate the FICA tax rate for each household type in each region.

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:fica_tax_rate`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :ftr | household | state | year | :fica_tax_rate | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `FICA_Tax`
- `Labor_Endowment`

## Calculation

The FICA tax rate is calculated as:

```math
{\\rm FICA\\_Tax\\_Rate} = \\frac{\\rm FICA\\_Tax}{\\sum_{\\rm Destinations} \\rm Labor\\_Endowment}
```
"""
function fica_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :fica_tax_rate,
    )

    df = innerjoin(
        table(HH, :Labor_Endowment) |> 
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :le),
        table(HH, :FICA_Tax; column = column, normalize= :Use),
        on = [:col, :region, :year]
        ) |>
        x -> transform(x,
            [:le, column] => ByRow((le, ft) -> ft / le) => output,
            :col => ByRow(y -> (:ftr, parameter)) => [:row, :parameter]
        )  |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df
end

"""
    average_labor_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :average_labor_tax_rate,
    )

Calculate the average labor tax rate for each household type in each region. 

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:average_labor_tax_rate`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :altr | household | state | year | :average_labor_tax_rate | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Average_Labor_Tax`
- `Labor_Endowment`

## Calculation

The average labor tax rate is calculated as:

```math
{\\rm Average\\_Labor\\_Tax\\_Rate} = \\frac{\\rm Average\\_Labor\\_Tax}{\\sum_{\\rm Destinations} \\rm Labor\\_Endowment}
```
"""
function average_labor_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :average_labor_tax_rate,
    )

    df = innerjoin(
        table(HH, :Labor_Endowment) |> 
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :le),
        table(HH, :Average_Labor_Tax; column = column, normalize= :Use),
        on = [:col, :region, :year]
        ) |>
        x -> transform(x,
            [:le, column] => ByRow((le, ot) -> ot / le) => output,
            :col => ByRow(y -> (:altr, parameter)) => [:row, :parameter]
        )  |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df

end

"""
    leisure_demand(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :leisure_demand,
    )

Calculate the leisure demand for households. The leisure demand is defined as a fraction
of the leisure supply, based on a labor supply income elasticity, which is set to 0.05.

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:leisure_demand`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :ld | household | state | year | :leisure_demand | value |

Non-symbol values are sets.

## Aggregate Parameters

- [`WiNDCHousehold.labor_supply`](@ref)

## Calculation

Leisure demand is calculated as:

```math
{\\rm Leisure\\_Demand} = \\epsilon \\cdot {\\rm Leisure\\_Supply}
```

where \\( \\epsilon \\) is the labor supply income elasticity (0.05).
"""
function leisure_demand(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :leisure_demand,
    )

    labor_supply_income_elasiticity = 0.05

    df = labor_supply(HH; column = column, output = output) |>
        x -> transform(x,
            output => ByRow(v -> labor_supply_income_elasiticity * v) => output,
            :col => ByRow(y -> (:ld, parameter)) => [:row, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df
end

"""
    capital_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :capital_tax_rate,
    )

Calculate the capital tax rate for each household type in each region. The capital tax rate
is defined as the ratio of `Capital_Tax` to `Capital_Demand`.

## Required Arguments

- `HH::HouseholdTable`: The household data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:capital_tax_rate`: The name of the parameter column.

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :ktr | household | state | year | :capital_tax_rate | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Capital_Tax`
- `Capital_Demand`

## Calculation

The capital tax rate is calculated as:

```math
{\\rm Capital\\_Tax\\_Rate} = \\frac{\\rm Capital\\_Tax}{\\rm Capital\\_Demand}
```
"""
function capital_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :capital_tax_rate,
    )

    df = innerjoin(
        table(HH, :Capital_Demand) |> 
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :ce),
        table(HH, :Capital_Tax; column = column, normalize= :Use),
        on = [:col, :region, :year]
        ) |>
        x -> transform(x,
            [:ce, column] => ByRow((ce, kt) -> kt / ce) => output,
            :col => ByRow(y -> (:ktr, parameter)) => [:row, :parameter]
        )  |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df

end

"""
    aggregate_transfer_payment(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :total_transfers,
    )

Aggregate the transfer payments for each household type in each region.

## Required Arguments

- `HH::HouseholdTable`: The household data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the aggregation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:total_transfers`: The name of the parameter column.

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :tt | household | state | year | :total_transfers | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Transfer_Payment`

## Calculation

The total transfer payments are calculated as:

```math
{\\rm Total\\_Transfers} = \\sum_{\\rm transfers} {\\rm Transfer\\_Payment}
```
"""
function aggregate_transfer_payment(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :total_transfers,
    )

    df = table(HH, :Transfer_Payment) |>
        x -> groupby(x, [:col, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :col => ByRow(y -> (:tt, parameter)) => [:row, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df

end

"""
    government_deficit(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :government_deficit,
    )

Calculate the government deficit for each year.

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:government_deficit`).

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :gd | :gd | :gd | year | :government_deficit | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Government_Final_Demand`
- `Transfer_Payment`
- `Average_Labor_Tax`
- `FICA_Tax`
- `Capital_Tax`
- `Output_Tax`
- `Tax`
- `Duty`

## Calculation

The government deficit is calculated as:

```math
{\\rm Government\\_Deficit} = - \\sum (Government\\_Final\\_Demand - Transfer\\_Payment - Average\\_Labor\\_Tax - FICA\\_Tax - Capital\\_Tax - Output\\_Tax - Tax - Duty)
```
"""
function government_deficit(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :government_deficit,
)

    df = table(HH,
        :Government_Final_Demand,
        :Transfer_Payment,
        :Average_Labor_Tax,
        :FICA_Tax,
        :Capital_Tax,
        :Output_Tax,
        :Tax,
        :Duty;
    ) |>
    x -> transform(x,
        [:parameter, column] => ByRow((p,v) -> p∈[:transfer_payment, :output_tax, :capital_tax] ? -v : v) => column
    ) |>
    x -> groupby(x, [:year]) |>
    x -> combine(x, column => (y -> -sum(y)) => output) |>
    x -> transform(x,
        :year => ByRow(y -> (:gd, :gd, :gd, parameter)) => [:row, :col, :region, :parameter]
    ) |>
    x -> select(x, [:row, :col, :region, :year, :parameter, output])

    return df
end

"""
    leisure_consumption_elasticity(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :leisure_consumption_elasticity,
    )

Calculate the leisure consumption elasticity for households.

## Arguments

- `HH::HouseholdTable`: The household table containing household data.

## Keyword Arguments

- `column::Symbol`: The column to aggregate (default is `:value`).
- `output::Symbol`: The name of the output column (default is `:value`).
- `parameter::Symbol`: The parameter to assign to the output (default is `:leisure_consumption_elasticity`).   

## Returns

A `DataFrame` with the following structure:

| row | col | region | year | parameter     | value |
|-----|-----|--------|------|---------------|-------|
| :els | household | state | year | :leisure_consumption_elasticity | value |

Non-symbol values are sets.

## Parameters

These values are extracted from the HouseholdTable

- `Personal_Consumption`

## Aggregate Parameters

- [`WiNDCHousehold.leisure_demand`](@ref)
- [`WiNDCHousehold.labor_supply`](@ref)

## Calculation

Leisure consumption elasticity is calculated as:

```math
{\\rm Leisure\\_Consumption\\_Elasticity} = \\epsilon \\cdot \\frac{(PCE + LD)}{PCE} \\cdot \\frac{LS}{LD}
```

where 

- \\( \\epsilon \\) is the leisure income elasticity (0.2),
- `PCE` is Personal Consumption Expenditure
- `LD` is Leisure Demand
- `LS` is Labor Supply
```
"""
function leisure_consumption_elasticity(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :leisure_consumption_elasticity,
    )

    leisure_income_elasticity = 0.2

    df = innerjoin(
        table(HH, :Personal_Consumption; normalize=:Use) |>
            x -> groupby(x, [:col, :region, :year]) |>
            x -> combine(x, column => sum => :pce),
        WiNDCHousehold.leisure_demand(HH; output = :ld, column = column) |>
            x -> select(x, :col, :region, :year, :ld),
        WiNDCHousehold.labor_supply(HH; output = :ls, column = column) |>
            x -> select(x, :col, :region, :year, :ls),
        on = [:col, :region, :year],
    ) |>
    x -> transform(x,
        [:pce, :ld, :ls] => ByRow((p, l, s) -> leisure_income_elasticity * (p+l)/p * s/l) => output,
        :col => ByRow(y -> (:els, parameter)) => [:row, :parameter]
    ) |>
    x -> select(x, :row, :col, :region, :year, :parameter, output) 


    return df
end