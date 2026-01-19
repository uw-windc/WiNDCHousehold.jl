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



"""
    RawHouseholdData
    RawHouseholdData(
        state_table::WiNDCRegional.State,
        cps::Dict{Symbol,DataFrame},
        nipa::DataFrame,
        acs::DataFrame,
        medicare::DataFrame,
        labor_tax_rates::DataFrame,
        cex_income_elasticities::DataFrame,
        pce_shares::DataFrame,
        capital_tax_rates::DataFrame;
        state_fips = WiNDCHousehold.load_state_fips(),
        income_categories = WiNDCHousehold.load_cps_income_categories(),
        state_abbreviations = WiNDCHousehold.load_state_fips(cols_to_keep = [:state, :abbreviation]),
    )

A container for all raw data used in WiNDCHousehold processing.

# Fields

- `state_fips::DataFrame`: State FIPS codes.
- `income_categories::DataFrame`: Income category mappings.
- `state_abbreviations::DataFrame`: State abbreviations.
- `income::DataFrame`: CPS income data.
- `numhh::DataFrame`: CPS number of households data.
- `nipa::DataFrame`: NIPA data.
- `acs_commute::DataFrame`: ACS commute data.
- `medicare::DataFrame`: Medicare data.
- `labor_tax_rates::DataFrame`: Labor tax rates.
- `capital_tax_rates::DataFrame`: Capital tax rates.
- `nipa_cps::DataFrame`: NIPA vs CPS income categories mapping.
- `windc_vs_nipa_income_categories::DataFrame`: WiNDC vs NIPA income categories mapping.
- `nipa_fringe::DataFrame`: NIPA fringe benefit markup data.
- `cps_data::DataFrame`: Processed CPS data.
- `cex_income_elasticities::DataFrame`: CEX income elasticities data.
- `pce_shares::DataFrame`: PCE shares data.
"""
struct RawHouseholdData
    state_fips::DataFrame
    income_categories::DataFrame
    state_abbreviations::DataFrame
    income::DataFrame                           # cps[:income]
    numhh::DataFrame                            # cps[:numhh]
    nipa::DataFrame                             # nipa
    acs_commute::DataFrame                      # acs
    medicare::DataFrame                         # medicare
    labor_tax_rates::DataFrame                  # labor_tax_rates
    capital_tax_rates::DataFrame                # capital_tax_rates

    nipa_cps::DataFrame                         # cps_vs_nipa_income_categories(cps[:income], nipa)
    windc_vs_nipa_income_categories::DataFrame  # windc_vs_nipa_income_categories(state_table, nipa)
    nipa_fringe::DataFrame                      # nipa_fringe_benefit_markup(nipa)
    cps_data::DataFrame                         # Combines income and labor_tax_rates. Needs to be function
    cex_income_elasticities::DataFrame          # cex_income_elasticities
    pce_shares::DataFrame                       # pce_shares

    function RawHouseholdData(
        state_table::WiNDCRegional.State,
        cps::Dict{Symbol,DataFrame},
        nipa::DataFrame,
        acs::DataFrame,
        medicare::DataFrame,
        labor_tax_rates::DataFrame,
        cex_income_elasticities::DataFrame,
        pce_shares::DataFrame,
        capital_tax_rates::DataFrame;
        state_fips = WiNDCHousehold.load_state_fips(),
        income_categories = WiNDCHousehold.load_cps_income_categories(),
        state_abbreviations = WiNDCHousehold.load_state_fips(cols_to_keep = [:state, :abbreviation]),
    )
        nipa_cps = WiNDCHousehold.cps_vs_nipa_income_categories(cps[:income], nipa)
        windc_vs_nipa_income_categories = WiNDCHousehold.windc_vs_nipa_income_categories(state_table, nipa)
        nipa_fringe = WiNDCHousehold.nipa_fringe_benefit_markup(nipa)

        cps_data = cps[:income] |>
            x -> innerjoin(x, income_categories, on = :source) |>
            x -> groupby(x, [:hh, :year, :state, :windc]) |>
            x -> combine(x, :value => sum => :value) |>
            x -> unstack(x, :windc, :value) |>
            x -> transform(x,
                :save => ByRow(y -> -y) => :save
            ) |>
            x -> stack(x, Not([:hh, :year, :state]), variable_name = :windc, value_name = :value)


        taxes = outerjoin(
                cps_data |>
                    x -> subset(x, :windc => ByRow(==("wages"))),
                labor_tax_rates |>
                    x -> subset(x,
                        :variable => ByRow(y -> y∈(["tl_avg", "tfica"]))
                    ) |>
                    x -> groupby(x, [:hh, :state]) |>
                    x -> combine(x, :labor_tax_rate => sum => :labor_tax_rate),
                on = [:hh, :state],
            ) |>
            x -> transform(x,
                [:value, :labor_tax_rate] => ByRow((v, l) -> -v * l) => :value,
                :windc => ByRow(y -> "labor_tax") => :windc
            ) |>
            x -> select(x, :hh, :year, :state, :windc, :value)

        cps_data = vcat(cps_data, taxes)



        return new(
            state_fips,
            income_categories,
            state_abbreviations,
            cps[:income],
            cps[:numhh],
            nipa,
            acs,
            medicare,
            labor_tax_rates,
            capital_tax_rates,
            nipa_cps,
            windc_vs_nipa_income_categories,
            nipa_fringe,
            cps_data,
            cex_income_elasticities,
            pce_shares
        )

    end
end