# Household Data Sets

This is a listing of the data sets and parameters available in the `WiNDCHousehold` module.
These can be accessed using the `sets` function.

```julia
using WiNDCHousehold
using WiNDCHousehold.WiNDCContainer

state_table, HH_Raw_Data = WiNDCHousehold.load_household_raw_data()
HH = build_household_table(state_table, HH_Raw_Data)

sets(HH) # Display all available sets as a DataFrame
```

Elements can be accessed using the `elements` function

```julia
elements(HH) # Display all elements as a DataFrame

elements(HH, :commodity) # Display all elements in the commodity set
```

The `domain` column indicates the column of the household table where the set is used.
For example, the `trade` set is used in the column dimension of the household table, 
while the `commodity` set is used in the row dimension.

```julia
table(HH) # The full household table

table(HH, :commodity) # The household table filtered to only the commodity rows

table(HH, :Intermediate_Demand, :Intermediate_Supply) # The Intermediate Demand and Supply parameters
```


## Sets

| name                    | domain    | description |
|-------------------------|-----------|-------------|
| trade                   | col       | Trade |
| export                  | col       | Exports |
| transport               | col       | Transport |
| import                  | col       | Imports |
| duty                    | col       | Duty |
| tax                     | col       | Tax |
| sector                  | col       | Sectors |
| margin                  | col       | Margin sectors |
| investment_final_demand | col       | Investment Final Demand |
| government_final_demand | col       | Government Final Demand |
| reexport                | col       | Reexports |
| local_demand            | col       | Local Demand |
| national_demand         | col       | National Demand |
| household               | col       | Household Categories |
| state                   | region    | States |
| labor_demand            | row       | Compensation of employees |
| commodity               | row       | Commodities |
| output_tax              | row       | Output tax |
| capital_demand          | row       | Gross operating surplus |
| capital_tax             | row       | Capital Tax |
| transfer_payment        | row       | Transfer Payments |
| destination             | row       | Destination Region |
| interest                | row       | Interest |
| marginal_labor_tax      | row       | Marginal Labor Tax |
| fica                    | row       | FICA Tax |
| average_labor_tax       | row       | Average Labor Tax |
| savings                 | row       | Savings |
| year                    | year      |          |


## Parameters

| name                    | domain    | description |
|-------------------------|-----------|-------------|
| Average_Labor_Tax       | parameter | Average Labor Tax |
| Capital_Demand          | parameter | Capital Demand |
| Capital_Tax             | parameter | Capital Tax |
| Duty                    | parameter | Duty |
| Export                  | parameter | Exports |
| FICA_Tax                | parameter | FICA Tax |
| Final_Demand            | parameter | Final demand |
| Government_Final_Demand | parameter | Government Final Demand |
| Household_Interest      | parameter | Household Interest |
| Household_Supply        | parameter | Household Supply |
| Import                  | parameter | Imports |
| Intermediate_Demand     | parameter | Intermediate Demand |
| Intermediate_Supply     | parameter | Intermediate Supply |
| Investment_Final_Demand | parameter | Investment Final Demand |
| Labor_Demand            | parameter | Labor Demand |
| Labor_Endowment         | parameter | Labor Endowment |
| Local_Demand            | parameter | Local Demand |
| Local_Margin_Supply     | parameter | Local Margin Supply |
| Margin_Demand           | parameter | Margin Demand |
| Margin_Supply           | parameter | Margin Supply |
| Marginal_Labor_Tax      | parameter | Marginal Labor Tax |
| National_Demand         | parameter | National Demand |
| National_Margin_Supply  | parameter | National Margin Supply |
| Other_Final_Demand      | parameter | Non-export components of final demand |
| Output_Tax              | parameter | Output Tax |
| Personal_Consumption    | parameter | Personal Consumption |
| Reexport                | parameter | Reexports |
| Savings                 | parameter | Savings |
| Supply                  | parameter | Supply (or output) sections of the IO table |
| Tax                     | parameter | Tax |
| Transfer_Payment        | parameter | Transfer Payments |
| Use                     | parameter | Use (or input) sections of the IO table |
| Value_Added             | parameter | Value added |

## Aggregate Parameters

- [`WiNDCHousehold.absorption`](@ref)
- [`WiNDCHousehold.aggregate_transfer_payment`](@ref)
- [`WiNDCHousehold.government_deficit`](@ref)
- [`WiNDCHousehold.labor_supply`](@ref)
- [`WiNDCHousehold.leisure_consumption_elasticity`](@ref)
- [`WiNDCHousehold.leisure_demand`](@ref)
- [`WiNDCHousehold.netports`](@ref)
- [`WiNDCHousehold.regional_local_supply`](@ref)
- [`WiNDCHousehold.regional_national_supply`](@ref)
- [`WiNDCHousehold.total_supply`](@ref)


### Tax Rates

All parameters are provided as values. To extract the tax rates, use the following aggregate parameters:

- [`WiNDCHousehold.average_labor_tax_rate`](@ref)
- [`WiNDCHousehold.capital_tax_rate`](@ref)
- [`WiNDCHousehold.duty_rate`](@ref)
- [`WiNDCHousehold.fica_tax_rate`](@ref)
- [`WiNDCHousehold.marginal_labor_tax_rate`](@ref)
- [`WiNDCHousehold.output_tax_rate`](@ref)