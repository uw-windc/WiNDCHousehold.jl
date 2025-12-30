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

A `DataFrame` with the aggregated labor supply for each region and year.

## Calculation

Labor supply is calculated as:
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
    total_supply(data::HouseholdTable; column::Symbol = :value, output::Symbol = :value)

Calculate the total supply for each commodity. The total supply is defined as the sum of:

- `Intermediate_Supply`
- `Household_Supply`
"""
function total_supply(data::HouseholdTable; column::Symbol = :value, output::Symbol = :value)
    return table(data, :Intermediate_Supply, :Household_Supply) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, :row => ByRow(y -> (:tot_sup, :total_supply)) => [:col, :parameter])
end

"""
    absorption(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value
    )

Calculate the absorption for each commodity in each region. The absorption is 
defined as the sum of:

- `Intermediate_Demand`
- `Other_Final_Demand`

Note that absorption is negative.
"""
function absorption(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        normalize::Bool = false
    )
    return table(HH, :Intermediate_Demand, :Other_Final_Demand) |>
        x -> groupby(x, [:row, :year, :region]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x, 
            :value => ByRow(y -> normalize ? -y : y) => :value,
            :row => ByRow(y -> (:abs, :absorption)) => [:col, :parameter]
        )
end

"""
    regional_local_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_local_supply,
        minimal::Bool = false
    )

Calculate the regional local supply for each commodity in each region. The regional local supply
is defined as the sum of `Local_Margin_Supply` and `Local_Demand`.
"""
function regional_local_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_local_supply,
        minimal::Bool = false
    )

    df = table(HH, :Local_Margin_Supply, :Local_Demand; normalize=:Use) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :row => ByRow(y -> (:rls, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df
end

"""
    netports(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :netport,
        minimal::Bool = false
    )

Calculate the netports for each commodity in each region. The netport
is defined as the difference between `Export` and `Reexport`.
"""
function netports(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :netport,
        minimal::Bool = false
    )

    df = table(HH, :Export, :Reexport; normalize = :Export) |>
        x -> groupby(x, [:row, :region, :year]) |>
        x -> combine(x, column => sum => output) |>
        x -> transform(x,
            :row => ByRow(y -> (:netport, parameter)) => [:col, :parameter]
        ) |>
        x -> select(x, [:row, :col, :region, :year, :parameter, output])

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df
end


"""
    regional_national_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_national_supply,
        minimal::Bool = false
    )

Calculate the regional national supply for each commodity in each region. The regional national supply
is defined as total supply minus net exports minus regional local supply.

"""
function regional_national_supply(
        HH::HouseholdTable;
        column::Symbol = :value,
        output::Symbol = :value,
        parameter::Symbol = :region_national_supply,
        minimal::Bool = false
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

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df

end



"""
    output_tax_rate(
            HH::HouseholdTable; 
            column::Symbol = :value, 
            output::Symbol = :value,
            parameter = :output_tax_rate,
            minimal::Bool = false
        )

Calculate the output tax rate for each commodity in each region. The output tax rate
is defined as the ratio of `Output_Tax` to the sum over commodities of `Intermediate_Supply`.

## Required Arguments

- `HH::HouseholdTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:output_tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].
"""
function output_tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :output_tax_rate,
        minimal::Bool = false
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

    if minimal
        df |>
            x -> select!(x, [:col, :region, :year, :parameter, output])
    end

    return df

end

"""
    tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :tax_rate,
        minimal::Bool = false
    )

Calculate the tax rate for each commodity in each region. The tax rate
is defined as the ratio of `Tax` to `Absorption`.

## Required Arguments

- `HH::HouseholdTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].
"""
function tax_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :tax_rate,
        minimal::Bool = false
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

    if minimal
        df |>
            x -> select!(x, [:row, :region, :year, :parameter, output])
    end

    return df

end



"""  
  duty_rate(
        HH::HouseholdTable; 
        column::Symbol = :value, 
        output::Symbol = :value,
        parameter = :duty_rate,
        minimal::Bool = false
    )
   
    
Calculate the duty rate for each commodity in each region. The duty rate
is defined as the ratio of `Duty` to `Import`.

## Required Arguments

- `HH::HouseholdTable`: The regional data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:duty_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].
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
        parameter = :marginal_labor_tax_rate,
        minimal::Bool = false
    )

Calculate the marginal labor tax rate for each household type in each region. The marginal labor tax rate
is defined as the ratio of `Marginal_Labor_Tax` to `Labor_Endowment`.

## Required Arguments

- `HH::HouseholdTable`: The household data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:marginal_labor_tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].

## Returns

A `DataFrame` with the marginal labor tax rate for each household type in each region.

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

Calculate the FICA tax rate for each household type in each region. The FICA tax rate
is defined as the ratio of `FICA_Tax` to `Labor_Endowment`.

## Required Arguments

- `HH::HouseholdTable`: The household data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:fica_tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].

## Returns

A `DataFrame` with the FICA tax rate for each household type in each region.

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

Calculate the average labor tax rate for each household type in each region. The average labor tax rate
is defined as the ratio of `Average_Labor_Tax` to `Labor_Endowment`.

## Required Arguments

- `HH::HouseholdTable`: The household data.

## Keyword Arguments

- `column::Symbol = :value`: The column to be used for the calculation.
- `output::Symbol = :value`: The name of the output column.
- `parameter::Symbol = `:average_labor_tax_rate`: The name of the parameter column.
- `minimal::Bool = false`: Whether to return a minimal output. If true, only the 
    essential columns are returned: [:col, :region, :year, :parameter, output].

## Returns

A `DataFrame` with the average labor tax rate for each household type in each region.

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

A `DataFrame` with the calculated leisure demand for each region and year.

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

A `DataFrame` with the capital tax rate for each household type in each region.

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

A `DataFrame` with the aggregated transfer payments for each household type in each region.

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

A `DataFrame` with the government deficit for each year.

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
        [:pce, :ld, :ls] => ByRow((p, l, s) -> leisure_income_elasticity * (p*l)/p * s/l) => output,
        :col => ByRow(y -> (:els, parameter)) => [:row, :parameter]
    ) |>
    x -> select(x, :row, :col, :region, :year, :parameter, output) 


    return df
end