"""
    calibration_model_1(HH::HouseholdTable, state_table::State, HH_Raw_Data::RawHouseholdData)

This is a placeholder docstring. It has been intentionally left blank as this
model can be simplified further.

To Do:

1. Add errors if model fails to solve
"""
function calibration_model_1(HH::HouseholdTable, state_table::State, HH_Raw_Data::RawHouseholdData)
    
    M = Model(Ipopt.Optimizer)

    regions = elements(HH, :state) |> x -> x[:, :name]
    households = elements(HH, :household) |> x -> x[:, :name]


    @variables(M, begin
        Taxes[r = regions, h = households]
        Foreign_Savings
        Transfer_Payments[r = regions, h = households]
        Other_Income[r = regions, h = households]
        Government_Transfers[r = regions, h = households] >= 0
        Consumption[r = regions, h = households] >= 0
        Wages[home = regions, work = regions, h = households] >= 0
        Interest[r = regions, h = households] >= 0
        Foreign_Capital_Ownership >= 0
        Savings[r = regions, h = households] >= 0
    end)

    vcat(
        WiNDCHousehold.adjusted_wages(HH, state_table, HH_Raw_Data; tax_adjustment = false) |>
            x -> transform(x,
                [:state, :hh] => ByRow((r,h) -> sum(Wages[r, :, Symbol(h)])) => :variable
            ) |>
            x -> select(x, :variable, :wage => :value),

        WiNDCHousehold.adjusted_capital_income(HH, state_table, HH_Raw_Data) |>
            x -> transform(x,
                [:state, :hh] => ByRow((r,h) -> sum(Interest[r, Symbol(h)])) => :variable
            ) |>
            x -> select(x, :variable, :capital => :value),

        HH_Raw_Data.acs_commute |>
            x -> transform(x, 
                [:home_state, :work_state] => ByRow((h,w) -> sum(Wages[h, w, :])) => :variable
            ) |>
            x -> select(x, :variable, :value),

        WiNDCHousehold.other_income(HH, state_table, HH_Raw_Data) |>
            x -> transform(x,
                [:state, :hh] => ByRow((r,h) -> Other_Income[r, Symbol(h)]) => :variable
            ) |>
            x -> select(x, :income => :value, :variable),
    ) |> Q ->
    @objective(M, Min, sum( abs(row[:value])*(row[:variable]/row[:value] -1)^2 for row in eachrow(Q) if row[:value] != 0 ) )


    HH_Raw_Data.labor_tax_rates |>
        x -> subset(x, :variable => ByRow(∈(["tfica","tl_avg"]))) |>
        x -> groupby(x, [:hh, :state]) |>
        x -> combine(x, :labor_tax_rate => sum => :tax_rate) |> df->
    @constraint(M, taxdef[row in eachrow(df)],
        Taxes[row[:state],Symbol(row[:hh])] == row[:tax_rate] * sum(Wages[row[:state], :, Symbol(row[:hh])])
    )


    table(state_table, :Personal_Consumption; normalize = :Use) |> 
        x -> groupby(x, [:region]) |>
        x -> combine(x, :value => sum => :value) |> df ->
    @constraint(M, consdef[row in eachrow(df)],
        sum(Consumption[row[:region], :]) == row[:value]
    )

    WiNDCHousehold.bls_distribution_expenditures(HH_Raw_Data) |> df ->
    @constraint(M, consdids[row in eachrow(df)],
        Consumption[row[:state], row[:hh]] == row[:bls]*sum(Consumption[row[:state], :])
    )

    table(HH, :Labor_Demand; normalize = :Use) |>
        x -> groupby(x, [:region]) |>
        x -> combine(x, :value => sum => :value) |> df ->
    @constraint(M, wagedef[row in eachrow(df)],
        sum(Wages[:, row[:region], :]) == row[:value]
    )

    HH_Raw_Data.cps_data |>
        x -> subset(x, :windc => ByRow(==("wages"))) |>
        x -> groupby(x, [:state]) |>
        x -> combine(x, 
            [:hh, :value] .=> identity .=> [:hh, :value],
            :value => sum => :total_wage
        ) |> df ->
    @constraint(M, wagedis[row in eachrow(df)],
        sum(Wages[row[:state], :, Symbol(row[:hh])]) * row[:total_wage] == row[:value] * sum(Wages[row[:state], :, :])
    )

    # Should be >=
    @constraint(M, commutedis[home = regions, h = households],
        Wages[home, home, h] >= sum(Wages[home, dest, h] for dest in regions if dest != home)
    )


    table(HH, :Capital_Demand, :Household_Supply; normalize=:Use) |>
        x -> combine(x, :value => sum => :value) |>
        x -> x[1, :value] |> Q ->
    @constraint(M, interestdef,
        sum(Interest[:, :]) + Foreign_Capital_Ownership == Q
    )


    HH_Raw_Data.cps_data |>
        x -> subset(x, :windc => ByRow(==("interest"))) |>
        x -> groupby(x, [:state]) |>
        x -> combine(x, 
            [:hh, :value] .=> identity .=> [:hh, :value],
            :value => sum => :total_interest
        ) |> df ->
    @constraint(M, interestdis[row in eachrow(df)],
        Interest[row[:state], Symbol(row[:hh])] * row[:total_interest] == row[:value] * sum(Interest[row[:state], :])
    )


    table(HH, :Investment_Final_Demand; normalize = :Use) |>
        x -> combine(x, :value => sum => :value) |>
        x -> x[1, :value] |> Q ->
    @constraint(M, savedef,
        sum(Savings[:, :]) + Foreign_Savings == Q
    )


    WiNDCHousehold.cbo_wealth_distribution() |> df ->
    @constraint(M, savedis[r = regions, row = eachrow(df)],
        Savings[r, row[:hh]] == row[:value]*sum(Savings[r, :])
    )


    #if false # capital_ownership == :partial
    #    @constraint(M, targetsave, 
    #        sum(Savings[:, :]) == 0.04 * sum(Interest[:, :])
    #    )
    #end


    @constraint(M, income_balance[r = regions, h = households],
        Transfer_Payments[r, h] + sum(Wages[r, :, h]) + Interest[r, h] == Consumption[r, h] + Savings[r, h] + Taxes[r, h]
    )




    #table(HH, :Transfer_Payment) |>
    #    x -> groupby(x, [:region, :col]) |>
    #    x -> combine(x,
    #        :row => identity => :row,
    #        :value => (y -> y/sum(y)) => :value
    #    ) |>
    #    x -> subset(x, :value => ByRow(!=(0))) |> df -> 0
    @constraint(M, disagtrn[r = regions, h = households],
        Government_Transfers[r, h] + Other_Income[r,h] == Transfer_Payments[r, h]
    )



    table(HH, :Transfer_Payment) |>
        x -> groupby(x, [:region, :col]) |>
        x -> combine(x,  
            :value => sum => :value
        ) |> df ->
    for row in eachrow(df)
        set_lower_bound(Government_Transfers[row[:region], row[:col]], .8*row[:value])
        set_upper_bound(Government_Transfers[row[:region], row[:col]], 1.2*row[:value])

        set_start_value(Transfer_Payments[row[:region], row[:col]], row[:value])
    end

    ## Note: This matches GAMS. But there is a comment says "set lower bound", which
    ## doesn't match the code.
    WiNDCHousehold.adjusted_consumption(HH, state_table, HH_Raw_Data) |> df ->
    for row in eachrow(df)
        set_start_value(Consumption[row[:state], Symbol(row[:hh])], row[:consumption])
    end



    fix.(Wages, 0; force=true)
    WiNDCHousehold.adjusted_wages(HH, state_table, HH_Raw_Data; tax_adjustment = false) |> df ->
    for row in eachrow(df)
        unfix(Wages[row[:state], row[:state], Symbol(row[:hh])])
        set_lower_bound(Wages[row[:state], row[:state], Symbol(row[:hh])], 0)
        set_start_value(Wages[row[:state], row[:state], Symbol(row[:hh])], row[:wage])
    end
    HH_Raw_Data.acs_commute |> df ->
    for row in eachrow(df)
        unfix.(Wages[row[:home_state], row[:work_state], :])
        set_lower_bound.(Wages[row[:home_state], row[:work_state], :], 0.05*row[:value]/5)
        set_start_value.(Wages[row[:home_state], row[:work_state], :], 0.5*row[:value]/5)
    end



    WiNDCHousehold.adjusted_capital_income(HH, state_table, HH_Raw_Data) |> df ->
    for row in eachrow(df)
        set_start_value(Interest[row[:state], Symbol(row[:hh])], row[:capital])

        set_lower_bound(Interest[row[:state], Symbol(row[:hh])], 0.75*row[:capital])
        set_upper_bound(Interest[row[:state], Symbol(row[:hh])], 1.25*row[:capital])
    end
    delete_upper_bound.(Interest[:, :hh5])


    HH_Raw_Data.cps_data |>
        x -> subset(x, :windc => ByRow(==("save"))) |> df ->
    for row in eachrow(df)
        set_start_value(Savings[row[:state], Symbol(row[:hh])], -row[:value])
        set_lower_bound(Savings[row[:state], Symbol(row[:hh])], -0.1*row[:value])
    end

    HH_Raw_Data.cps_data |>
        x -> subset(x, :windc => ByRow(==("labor_tax"))) |> df ->
    for row in eachrow(df)
        set_start_value(Taxes[row[:state], Symbol(row[:hh])], -row[:value])
    end


    fix(Foreign_Capital_Ownership, 0; force=true);
    fix(Foreign_Savings, 0; force=true);


    set_attribute(M, "max_iter", 500)

    optimize!(M)

    return M
end