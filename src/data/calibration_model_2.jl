"""
    calibration_model_2(
        HH::HouseholdTable, 
        state_table::State, 
        HH_Raw_Data::RawHouseholdData,
        calibration_model_1::JuMP.Model
    )

This is a placeholder docstring. It has been intentionally left blank as this
model can be simplified further.

To Do:

1. Add errors if model fails to solve
"""
function calibration_model_2(
        HH::HouseholdTable, 
        state_table::State, 
        HH_Raw_Data::RawHouseholdData,
        calibration_model_1::JuMP.Model
    )

    regions = elements(HH, :state) |> x -> x[:, :name]
    households = elements(HH, :household) |> x -> x[:, :name]
    commodities = elements(HH, :commodity) |> x -> x[:, :name]


    cex_income_elasticities = HH_Raw_Data.cex_income_elasticities
    pce_share = HH_Raw_Data.pce_shares
    

    eta0 = outerjoin(
        pce_share,
        cex_income_elasticities,
        on = :cex
    ) |>
    x -> groupby(x, :naics) |>
    x -> combine(x,
        [:value, :elast] => ((v,e) -> sum(v.*e)) => :eta
    ) |>
    x -> transform(x,
        :naics => ByRow(Symbol) => :naics
    ) 

    theta0 = table(state_table, :Personal_Consumption; normalize = :Use) |>
        x -> groupby(x, [:region, :year]) |>
        x -> combine(x,
            :row => identity => :row,
            :value => (y -> y./sum(y)) => :theta
        )
    

    consumption = DataFrame(vec([
        (region = r, hh = hh, cons = value(calibration_model_1[:Consumption][r, hh]))
        for r in regions, hh in households
    ]))

    pop0 = HH_Raw_Data.numhh |>
        x -> transform(x,
            :hh => ByRow(Symbol) => :hh
        )

    incomeindex = outerjoin(
        consumption,
        pop0,
        on = [:region => :state, :hh => :hh]
    ) |>
    x -> combine(x,
        [:region, :hh] .=> identity .=> [:region, :hh],
        [:cons, :numhh] => ((c,p) -> c/sum(c) .* sum(p)./p) => :incomeindex
    )

    theta = outerjoin(
        theta0,
        incomeindex,
        on = :region
    ) |>
    x -> leftjoin(
        x,
        eta0,
        on = :row => :naics
    ) |>
    x -> groupby(x, [:region, :hh]) |>
    x -> combine(x,
        :row => identity => :row,
        [:theta, :incomeindex, :eta] => ((v,i,e) -> v.*i.^e / sum(v.*i.^e)) => :theta
    ) 

        
    Calib3 = Model(Ipopt.Optimizer)


    @variable(
        Calib3, 
        CD[r = regions, g = commodities, h = households] >= 1e-5
    )


    fix.(CD, 0; force = true)
    outerjoin(
        theta,
        consumption,
        on = [:region, :hh]
    ) |>
    x -> transform(x,
        [:theta, :cons] => ByRow(*) => :target
    ) |>
    x -> select(x, :region, :row, :hh, :target) |> df -> 
    for row in eachrow(df)
        unfix(CD[row[:region], row[:row], row[:hh]])
        set_lower_bound(CD[row[:region], row[:row], row[:hh]], 1e-5)
        set_start_value(CD[row[:region], row[:row], row[:hh]], row[:target])
    end


    outerjoin(
        theta,
        consumption,
        on = [:region, :hh]
    ) |>
    x -> transform(x,
        [:theta, :cons] => ByRow(*) => :target
    ) |>
    x -> select(x, :region, :row, :hh, :target) |> df -> 
    @objective(
        Calib3,
        Min,
        sum( (CD[row[:region], row[:row], row[:hh]] - row[:target])^2 - log(CD[row[:region], row[:row], row[:hh]]) for row in eachrow(df) )
    )

    table(state_table, :Personal_Consumption; normalize = :Use) |> df ->
    @constraint(Calib3, market[row = eachrow(df)], 
        sum(CD[row[:region], row[:row], hh] for hh∈households) == row[:value]
    )

    consumption |>
        x -> subset(x, :hh => ByRow(∉([:hh5]))) |> df ->
    @constraint(Calib3, budget[row = eachrow(df)],
        sum(CD[row[:region], g, row[:hh]] for g∈commodities) == row[:cons]
    )



    optimize!(Calib3)

    return Calib3
end