abstract type AbstractHouseholdTable <: WiNDCtable end

WiNDCContainer.domain(data::AbstractHouseholdTable) = [:row, :col, :region, :year]
WiNDCContainer.base_table(data::AbstractHouseholdTable) = data.data
WiNDCContainer.sets(data::AbstractHouseholdTable) = data.sets
WiNDCContainer.elements(data::AbstractHouseholdTable) = data.elements

"""
    Household

The primary container for WiNDC household disaggregation data tables. There are 
three fields, all dataframes:
- `data`: The main data table.
- `sets`: The sets table, describing the different sets used in the model.
- `elements`: The elements table, describing the different elements in the model.
"""
struct HouseholdTable <: AbstractHouseholdTable
    data::DataFrame
    sets::DataFrame
    elements::DataFrame
end