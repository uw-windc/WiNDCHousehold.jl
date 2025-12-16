
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
"""
function initialize_table(state_table::State)

    parameters_to_keep = [
        :Capital_Demand
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
        :capital_demand
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
    S = sets(state_table, sets_to_keep..., parameters_to_keep...)
    E = elements(state_table, sets_to_keep..., parameters_to_keep...) |>
        x -> subset(x, :name => ByRow(!=(:personal_consumption))) 


    HH = HouseholdTable(T, S, E; regularity_check=true)

    return HH

end