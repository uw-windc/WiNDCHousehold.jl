"""
    household_model(
        HH::HouseholdTable;
        etaK::Real = 4.0,
        )

Constructs a WiNDCHousehold MPSGE model from a WiNDCHousehold data table.

## Arguments

- `HH::HouseholdTable`: A WiNDCHousehold data table.

## Keyword Arguments

- `etaK::Real = 4.0`: Capital transformation elasticity

## Returns

- `M::MPSGEModel`: An MPSGE model representing the household sector.

## Parameters

The initial values of the parameters are given by [`parameter_data`](@ref). The 
parameters included in the model are:

- `OTR[r=states, s=sectors]`:  Output tax rate
- `TR[r=states, g=commodities]`: Tax rate
- `DR[r=states, g=commodities]`: Duty rate
- `TK[r=states, s=sectors]`:  Capital tax rate
- `FICA[r=states, h=households]`:  FICA tax rate
- `LTR[r=states, h=households]`:  Labor tax rate

If the model is denoted `M`, these parameters can be accessed as `M[:OTR]`, 
`M[:TR]`, etc. 


## Variables

### Sectors

- `Y[r=states, s=sectors]`:" Production
- `X[r=states, g=commodities]`:" Disposition
- `A[r=states, g=commodities]`:" Absorption
- `C[r=states, h=households]`:" Household Consumption
- `MS[r=states, m=margins]`:" Margin supply
- `LS[r=states, h=households]`:" Labor supply
- `KS`: Aggregate Capital stock

### Commodities

- `PA[r=states, g=commodities]`:" Regional market (input)
- `PC[r=states, h=households]`:" Consumer price index
- `PD[r=states, g=commodities]`:" Local market price
- `PFX`: Foreign exchange
- `PK`: Aggregate return to capital
- `PL[r=states]`:" Wage rate
- `PLS[r=states, h=households]`:" Leisure price
- `PM[r=states, m=margins]`:" Margin price
- `PN[g=commodities]`:" National market
- `PY[r=states, g=commodities]`:" Regional market (output)
- `RK[r=states, s=sectors]`:" Sectoral rental rate
- `RKS`: Capital stock

### Consumers

- `RA[r=states, h=households]`: Representative agent
- `NYSE`: Aggregate capital owner
- `INVEST`: Aggregate investor
- `GOVT`: Aggregate government

### Auxiliaries

All auxiliaries start with a value of 1. 

- `SAVERATE`: Domestic saving rate
- `TRANS`: Budget balance rationing variable
- `SSK`: Steady-state capital stock
- `CPI`: Consumer price index


## Production Blocks

### Y - Sectoral Production in Each State

[`WiNDCHousehold.sectoral_output`](@ref)
```julia
sectoral_output(HH; output = :DefaultDict) |> Q->
@production(M, Y[r=states, s=sectors], [t=0, s=0, va=>s=1], begin
    @output(PY[r,g=commodities], Q[g, s, r, :intermediate_supply],                     t,  taxes = [Tax(GOVT, M[:OTR][r,s])], reference_price = 1-Q[:otr, s, r, :output_tax_rate])
    @input(PA[r, g=commodities], Q[g, s, r, :intermediate_demand],                     s)
    @input(PL[r],            sum(Q[l, s, r, :labor_demand] for l in labor_demand),     va)
    @input(RK[r, s],         sum(Q[k, s, r, :capital_demand] for k in capital_demand), va, taxes = [Tax(GOVT, M[:TK][r,s])], reference_price = 1 + Q[:ktr, s, r, :capital_tax_rate])
end)
```

### X - Commodity Disposition in Each State

[`WiNDCHousehold.disposition_data`](@ref)
```julia
disposition_data(HH; output = :DefaultDict) |> Q->
@production(M, X[r=states, g=commodities], [s=0, t=4], begin
    @output(PFX,      Q[g, r, :netport],                t)
    @output(PN[g],    Q[g, r, :region_national_supply], t)
    @output(PD[r, g], Q[g, r, :region_local_supply],    t)
    @input(PY[r, g],  Q[g, r, :total_supply],           s)
end)
```

### A - Armington Commodity Absorption in Each State

[`WiNDCHousehold.armington_data`](@ref)
```julia
armington_data(HH; output = :DefaultDict) |> Q-> 
@production(M, A[r=states, g=commodities], [t=0, s=0, dm => s = 2, d=>dm=4], begin
    @output(PA[r, g],        Q[g, :abs, r, :absorption],                  t, taxes = [Tax(GOVT, M[:TR][r,g])], reference_price = 1 - Q[g, :tr, r, :tax_rate])
    @output(PFX,             Q[g, :reexport, r, :reexport],               t)
    @input(PN[g],            Q[g, :national_demand, r, :national_demand], d)
    @input(PD[r, g],         Q[g, :local_demand, r, :local_demand],       d)
    @input(PFX,          sum(Q[g, i, r, :import] for i in imports),       dm, taxes = [Tax(GOVT, M[:DR][r,g])], reference_price = 1 + Q[g, :dr, r, :duty_rate])
    @input(PM[r, m=margins], Q[g, m, r, :margin_demand],                  s)
end)
```

### MS - Margin Supply and Demand in Each State

[`WiNDCHousehold.margin_supply_demand`](@ref)
```julia
margin_supply_demand(HH; output = :DefaultDict) |> Q->
@production(M, MS[r=states, m=margins], [t=0, s=0], begin
    @output(PM[r, m],        sum(Q[g, m, r, :margin_demand] for g in commodities), t)
    @input(PN[g=commodities],    Q[g, m, r, :national_margin_supply],              s)
    @input(PD[r, g=commodities], Q[g, m, r, :local_margin_supply],                 s)
end)
```

### C - Household Consumption in Each State

[`WiNDCHousehold.consumption_data`](@ref)
```julia
consumption_data(HH; output = :DefaultDict) |> Q->
@production(M, C[r=states, h=households], [t=0, s=1], begin
    @output(PC[r, h],        sum(Q[g, h, r, :personal_consumption] for g in commodities), t)
    @input(PA[r, g=commodities], Q[g, h, r, :personal_consumption],                       s)
end)
```

### LS - Labor Supply in Each State

[`WiNDCHousehold.labor_data`](@ref)
```julia
labor_data(HH; output = :DefaultDict) |> Q->
@production(M, LS[r=states, h=households], [t=0, s=1], begin
    @output(PL[q=states], Q[q, h, r, :labor_endowment], t, taxes = [Tax(GOVT, M[:LTR][r,h] + M[:FICA][r,h])], reference_price = 1 - Q[:ltr, h, r, :marginal_labor_tax_rate] - Q[:ftr, h, r, :fica_tax_rate])
    @input(PLS[r, h],     Q[:ls, h, r, :labor_supply], s)
end)
```

### KS - Capital Stock Transformation

[`WiNDCHousehold.capital_stock_data`](@ref)
```julia
capital_stock_data(HH; output = :DefaultDict) |> Q->
@production(M, KS, [t=etaK, s=1], begin
    @output(RK[r=states, s=sectors], Q[s, r, :capital_demand], t)
    @input(RKS,                  sum(Q[ss, rr, :capital_demand] for ss in sectors, rr in states), s)
end)
```

## Demands

### RA - Representative Agent Demand in Each State

[`WiNDCHousehold.representative_agent_data`](@ref)
```julia
representative_agent_data(HH; output = :DefaultDict) |> Q->
@demand(M, RA[r=states, h=households], begin
    @final_demand(PC[r, h], sum(Q[g, h, r, :personal_consumption] for g in commodities; init=0))
    @final_demand(PLS[r, h],    Q[:ld, h, r, :leisure_demand])
    @endowment(PLS[r, h],       Q[:ld, h, r, :leisure_demand] + Q[:ls, h, r, :labor_supply])
    @endowment(PFX,         sum(Q[trn, h, r, :transfer_payment] for trn in transfers; init=0))
    @endowment(PLS[r, h],   (M[:LTR][r,h] - Q[:altr, h, r, :average_labor_tax_rate]) * sum(Q[dest, h, r, :labor_endowment] for dest in states; init=0))
    @endowment(PK,              Q[:interest, h, r, :household_interest])
    @endowment(PFX,            -Q[:savings, h, r, :savings]*SAVERATE)
end, elasticity = Q[:els, h, r, :leisure_consumption_elasticity])
```

### NYSE - Aggregate Capital Owner Demand

[`WiNDCHousehold.NYSE_data`](@ref)
```julia
NYSE_data(HH; output = :DefaultDict) |> Q->
@demand(M, NYSE, begin
    @final_demand(PK,                       sum(Q[row, col, r, :household_supply] for row in commodities, col in personal_consumption, r in states; init=0) + sum(Q[row, col, r, :capital_demand] for row in capital_demand, col in sectors, r in states; init=0))
    @endowment(PY[r=states, g=commodities], sum(Q[g, col, r, :household_supply] for col in personal_consumption; init=0))
    @endowment(RKS,                         sum(Q[k, s, r, :capital_demand] for k in capital_demand, s in sectors, r in states;init=0)*SSK)
end)
```

### INVEST - Aggregate Investor Demand

[`WiNDCHousehold.invest_data`](@ref)
```julia
invest_data(HH; output = :DefaultDict) |> Q->
@demand(M, INVEST, begin
    @final_demand(PA[r=states, g=commodities], Q[g, :invest, r, :investment_final_demand])
    @endowment(PFX,                        sum(Q[:savings, h, r, :savings] for h in households, r in states; init=0)*SAVERATE)
    @endowment(PFX, 0) # fsav0
end)
```

### GOVT - Aggregate Government Demand

[`WiNDCHousehold.government_data`](@ref)
```julia
government_data(HH; output = :DefaultDict) |> Q->
@demand(M, GOVT, begin
    @final_demand(PA[r=states, g=commodities], Q[g, :govern, r, :government_final_demand])
    @endowment(PFX,                 -TRANS*sum(Q[trn, h, r, :transfer_payment] for trn in transfers, h in households, r in states; init=0))
    @endowment(PFX,                            Q[:gd, :gd, :gd, :government_deficit])
    @endowment(PLS[r=states, h=households], -(M[:LTR][r,h] - Q[:altr, h, r, :average_labor_tax_rate]) * sum(Q[dest, h, r, :labor_endowment] for dest in states; init=0))
end)
```

## Auxiliary Constraints

### SSK - Steady-State Capital Stock Constraint

[`WiNDCHousehold.ssk_data`](@ref)
```julia
ssk_data(HH; output = :DefaultDict) |> Q->
@aux_constraint(M, SSK, 
    sum(Q[g, :invest, r, :investment_final_demand]*(PA[r,g]-RKS) for g in commodities, r in states)
)
```

### SAVERATE - Domestic Saving Rate Constraint

[`WiNDCHousehold.saverate_data`](@ref)
```julia
saverate_data(HH; output = :DefaultDict) |> Q->
@aux_constraint(M, SAVERATE, 
    INVEST - sum(Q[g, :invest, r, :investment_final_demand]*PA[r,g]*SSK for g in commodities, r in states)
)
```

### TRANS - Budget Balance Rationing Variable Constraint

[`WiNDCHousehold.trans_data`](@ref)
```julia
trans_data(HH; output = :DefaultDict) |> Q->
@aux_constraint(M, TRANS,
    GOVT - sum(PA[r,g]*Q[g, :govern, r, :government_final_demand] for g in commodities, r in states)
)
```

### CPI - Consumer Price Index Constraint

[`WiNDCHousehold.cpi_data`](@ref)
```julia
cpi_data(HH; output = :DefaultDict) |> Q->
@aux_constraint(M, CPI,
    CPI - sum(PC[r,h]*Q[h, r, :personal_consumption] for h in households, r in states)/ sum(Q[h, r, :personal_consumption] for h in households, r in states; init=0)
)
```
"""
function household_model(
        HH::HouseholdTable;
        etaK::Real = 4.0,
        )

    states = elements(HH, :state) |> x -> x[!, :name]
    sectors = elements(HH, :sector) |> x -> x[!, :name]
    commodities = elements(HH, :commodity) |> x -> x[!, :name]
    margins = elements(HH, :margin) |> x -> x[!, :name]
    labor_demand = elements(HH, :labor_demand) |> x -> x[!, :name]
    capital_demand = elements(HH, :capital_demand) |> x -> x[!, :name]
    imports = elements(HH, :import) |> x -> x[!, :name]
    transfers = elements(HH, :transfer_payment) |> x -> x[!, :name]
    personal_consumption = elements(HH, :personal_consumption) |> x -> x[!, :name]
    households = elements(HH, :household) |> x -> x[!, :name]
    capital_demand = elements(HH, :capital_demand) |> x -> x[!, :name]

    
        
    M = MPSGEModel()

    parameter_data(HH; output = :DefaultDict) |> Q->
    @parameters(M, begin
        OTR[r=states, s=sectors], Q[:otr, s, r, :output_tax_rate], (description = "Output tax rate",)
        TR[r=states, g=commodities], Q[g, :tr, r, :tax_rate], (description = "Tax rate",)
        DR[r=states, g=commodities], Q[g, :dr, r, :duty_rate], (description = "Duty rate",)
        TK[r=states, s=sectors], Q[:ktr, s, r, :capital_tax_rate], (description = "Capital tax rate",)
        FICA[r=states, h=households], Q[:ftr, h, r, :fica_tax_rate], (description = "FICA tax rate",)
        LTR[r=states, h=households], Q[:ltr, h, r, :marginal_labor_tax_rate], (description = "Labor tax rate",)
    end)


    @sectors(M, begin
        Y[r=states, s=sectors], (description = "Production",)
        X[r=states, g=commodities], (description = "Disposition",)
        A[r=states, g=commodities], (description = "Absorption",)
        C[r=states, h=households], (description = "Household Consumption",)
        MS[r=states, m=margins], (description = "Margin supply",)
        LS[r=states, h=households], (description = "Labor supply",)
        KS, (description = "Aggregate Capital stock",)
    end)


    @commodities(M, begin
        PA[r=states, g=commodities], (description = "Regional market (input)",)
        PC[r=states, h=households], (description = "Consumer price index",)
        PD[r=states, g=commodities], (description = "Local market price",)
        PFX, (description = "Foreign exchange",)
        PK, (description = "Aggregate return to capital",)
        PL[r=states], (description = "Wage rate",)
        PLS[r=states, h=households], (description = "Leisure price",)
        PM[r=states, m=margins], (description = "Margin price",)
        PN[g=commodities], (description = "National market",)
        PY[r=states, g=commodities], (description = "Regional market (output)",)
        RK[r=states, s=sectors], (description = "Sectoral rental rate",)
        RKS, (description = "Capital stock",)
    end)


    @consumers(M, begin
        RA[r=states, h=households], (description = "Representative agent",)
        NYSE	, (description = "Aggregate capital owner")
        INVEST	, (description = "Aggregate investor")
        GOVT	, (description = "Aggregate government")
       # ROW$fint0, (description = "Aggregate rest of world")
    end)

    @auxiliaries(M, begin
        SAVERATE, (start = 1, description = "Domestic saving rate",)
        TRANS, (start = 1, description = "Budget balance rationing variable")
        SSK, (start = 1, description = "Steady-state capital stock")
        CPI, (start = 1, description = "Consumer price index")
    end)


    sectoral_output(HH; output = :DefaultDict) |> Q->
    @production(M, Y[r=states, s=sectors], [t=0, s=0, va=>s=1], begin
        @output(PY[r,g=commodities], Q[g, s, r, :intermediate_supply],                     t,  taxes = [Tax(GOVT, M[:OTR][r,s])], reference_price = 1-Q[:otr, s, r, :output_tax_rate])
        @input(PA[r, g=commodities], Q[g, s, r, :intermediate_demand],                     s)
        @input(PL[r],            sum(Q[l, s, r, :labor_demand] for l in labor_demand),     va)
        @input(RK[r, s],         sum(Q[k, s, r, :capital_demand] for k in capital_demand), va, taxes = [Tax(GOVT, M[:TK][r,s])], reference_price = 1 + Q[:ktr, s, r, :capital_tax_rate])
    end)

        
    disposition_data(HH; output = :DefaultDict) |> Q->
    @production(M, X[r=states, g=commodities], [s=0, t=4], begin
        @output(PFX,      Q[g, r, :netport],                t)
        @output(PN[g],    Q[g, r, :region_national_supply], t)
        @output(PD[r, g], Q[g, r, :region_local_supply],    t)
        @input(PY[r, g],  Q[g, r, :total_supply],           s)
    end)

    armington_data(HH; output = :DefaultDict) |> Q-> 
    @production(M, A[r=states, g=commodities], [t=0, s=0, dm => s = 2, d=>dm=4], begin
        @output(PA[r, g],        Q[g, :abs, r, :absorption],                  t, taxes = [Tax(GOVT, M[:TR][r,g])], reference_price = 1 - Q[g, :tr, r, :tax_rate])
        @output(PFX,             Q[g, :reexport, r, :reexport],               t)
        @input(PN[g],            Q[g, :national_demand, r, :national_demand], d)
        @input(PD[r, g],         Q[g, :local_demand, r, :local_demand],       d)
        @input(PFX,          sum(Q[g, i, r, :import] for i in imports),       dm, taxes = [Tax(GOVT, M[:DR][r,g])], reference_price = 1 + Q[g, :dr, r, :duty_rate])
        @input(PM[r, m=margins], Q[g, m, r, :margin_demand],                  s)
    end)

    margin_supply_demand(HH; output = :DefaultDict) |> Q->
    @production(M, MS[r=states, m=margins], [t=0, s=0], begin
        @output(PM[r, m],        sum(Q[g, m, r, :margin_demand] for g in commodities), t)
        @input(PN[g=commodities],    Q[g, m, r, :national_margin_supply],              s)
        @input(PD[r, g=commodities], Q[g, m, r, :local_margin_supply],                 s)
    end)

    consumption_data(HH; output = :DefaultDict) |> Q->
    @production(M, C[r=states, h=households], [t=0, s=1], begin
        @output(PC[r, h],        sum(Q[g, h, r, :personal_consumption] for g in commodities), t)
        @input(PA[r, g=commodities], Q[g, h, r, :personal_consumption],                       s)
    end)

    labor_data(HH; output = :DefaultDict) |> Q->
    @production(M, LS[r=states, h=households], [t=0, s=1], begin
        @output(PL[q=states], Q[q, h, r, :labor_endowment], t, taxes = [Tax(GOVT, M[:LTR][r,h] + M[:FICA][r,h])], reference_price = 1 - Q[:ltr, h, r, :marginal_labor_tax_rate] - Q[:ftr, h, r, :fica_tax_rate])
        @input(PLS[r, h],     Q[:ls, h, r, :labor_supply],  s)
    end)


    capital_stock_data(HH; output = :DefaultDict) |> Q->
    @production(M, KS, [t=etaK, s=1], begin
        @output(RK[r=states, s=sectors], Q[s, r, :capital_demand], t)
        @input(RKS,                  sum(Q[ss, rr, :capital_demand] for ss in sectors, rr in states), s)
    end)



    representative_agent_data(HH; output = :DefaultDict) |> Q->
    @demand(M, RA[r=states, h=households], begin
        @final_demand(PC[r, h], sum(Q[g, h, r, :personal_consumption] for g in commodities; init=0))
        @final_demand(PLS[r, h],    Q[:ld, h, r, :leisure_demand])
        @endowment(PLS[r, h],       Q[:ld, h, r, :leisure_demand] + Q[:ls, h, r, :labor_supply])
        @endowment(PFX,         sum(Q[trn, h, r, :transfer_payment] for trn in transfers; init=0))
        @endowment(PLS[r, h],   (M[:LTR][r,h] - Q[:altr, h, r, :average_labor_tax_rate]) * sum(Q[dest, h, r, :labor_endowment] for dest in states; init=0))
        @endowment(PK,              Q[:interest, h, r, :household_interest])
        @endowment(PFX,            -Q[:savings, h, r, :savings]*SAVERATE)
    end, elasticity = Q[:els, h, r, :leisure_consumption_elasticity])

    NYSE_data(HH; output = :DefaultDict) |> Q->
    @demand(M, NYSE, begin
        @final_demand(PK,                       sum(Q[row, col, r, :household_supply] for row in commodities, col in personal_consumption, r in states; init=0) + sum(Q[row, col, r, :capital_demand] for row in capital_demand, col in sectors, r in states; init=0))
        @endowment(PY[r=states, g=commodities], sum(Q[g, col, r, :household_supply] for col in personal_consumption; init=0))
        @endowment(RKS,                         sum(Q[k, s, r, :capital_demand] for k in capital_demand, s in sectors, r in states;init=0)*SSK)
    end)

    invest_data(HH; output = :DefaultDict) |> Q->
    @demand(M, INVEST, begin
        @final_demand(PA[r=states, g=commodities], Q[g, :invest, r, :investment_final_demand])
        @endowment(PFX,                        sum(Q[:savings, h, r, :savings] for h in households, r in states; init=0)*SAVERATE)
        @endowment(PFX, 0) # fsav0
    end)

    government_data(HH; output = :DefaultDict) |> Q->
    @demand(M, GOVT, begin
        @final_demand(PA[r=states, g=commodities], Q[g, :govern, r, :government_final_demand])
        @endowment(PFX,                 -TRANS*sum(Q[trn, h, r, :transfer_payment] for trn in transfers, h in households, r in states; init=0))
        @endowment(PFX,                            Q[:gd, :gd, :gd, :government_deficit])
        @endowment(PLS[r=states, h=households], -(M[:LTR][r,h] - Q[:altr, h, r, :average_labor_tax_rate]) * sum(Q[dest, h, r, :labor_endowment] for dest in states; init=0))
    end)


    ssk_data(HH; output = :DefaultDict) |> Q->
    @aux_constraint(M, SSK, 
        sum(Q[g, :invest, r, :investment_final_demand]*(PA[r,g]-RKS) for g in commodities, r in states)
    )

    saverate_data(HH; output = :DefaultDict) |> Q->
    @aux_constraint(M, SAVERATE, 
        INVEST - sum(Q[g, :invest, r, :investment_final_demand]*PA[r,g]*SSK for g in commodities, r in states)
    )

    trans_data(HH; output = :DefaultDict) |> Q->
    @aux_constraint(M, TRANS,
        GOVT - sum(PA[r,g]*Q[g, :govern, r, :government_final_demand] for g in commodities, r in states)
    )

    cpi_data(HH; output = :DefaultDict) |> Q->
    @aux_constraint(M, CPI,
        CPI - sum(PC[r,h]*Q[h, r, :personal_consumption] for h in households, r in states)/ sum(Q[h, r, :personal_consumption] for h in households, r in states; init=0)
    )

    return M

end


"""
    parameter_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

Returns initial parameter data for the household model.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
vcat(
    output_tax_rate(data),
    tax_rate(data),
    duty_rate(data),
    capital_tax_rate(data),
    fica_tax_rate(data),
    marginal_labor_tax_rate(data)
    )
```

## Aggregate Data

- [`WiNDCHousehold.output_tax_rate`](@ref)
- [`WiNDCHousehold.tax_rate`](@ref)
- [`WiNDCHousehold.duty_rate`](@ref)
- [`WiNDCHousehold.capital_tax_rate`](@ref)
- [`WiNDCHousehold.fica_tax_rate`](@ref)
- [`WiNDCHousehold.marginal_labor_tax_rate`](@ref)
"""
function parameter_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = vcat(
        WiNDCHousehold.output_tax_rate(data),
        WiNDCHousehold.tax_rate(data),
        WiNDCHousehold.duty_rate(data),
        WiNDCHousehold.capital_tax_rate(data),
        WiNDCHousehold.fica_tax_rate(data),
        WiNDCHousehold.marginal_labor_tax_rate(data)
        ) 

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end


"""
    sectoral_output(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts sectoral output-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the sectoral output data.

## Data

```julia
vcat(
    table(data, 
        :Intermediate_Supply, 
        :Intermediate_Demand, 
        :Labor_Demand, 
        :Capital_Demand;
        normalize = :Use
        ),
    output_tax_rate(data),
    capital_tax_rate(data)
    )
```

## Aggregate Data

- [`WiNDCHousehold.output_tax_rate`](@ref)
- [`WiNDCHousehold.capital_tax_rate`](@ref)

"""
function sectoral_output(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = vcat(
        table(data, 
            :Intermediate_Supply, 
            :Intermediate_Demand, 
            :Labor_Demand, 
            :Capital_Demand;
            normalize = :Use
            ),
        output_tax_rate(data),
        capital_tax_rate(data)
        )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end



"""
    disposition_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts disposition-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    vcat(
        regional_local_supply(data),
        netports(data),
        total_supply(data),
        regional_national_supply(data)
        )
```

## Aggregate Data

- [`WiNDCHousehold.regional_local_supply`](@ref)
- [`WiNDCHousehold.netports`](@ref)
- [`WiNDCHousehold.total_supply`](@ref)
- [`WiNDCHousehold.regional_national_supply`](@ref)
"""
function disposition_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = vcat(
        regional_local_supply(data),
        netports(data),
        total_supply(data),
        regional_national_supply(data)
        )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    armington_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts Armington-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    vcat(
        absorption(data; normalize = true),
        table(data, :Reexport, :National_Demand, :Local_Demand, :Import, :Margin_Demand; normalize = :Use),
        tax_rate(data),
        duty_rate(data)
    )
```

## Aggregate data

- [`WiNDCHousehold.absorption`](@ref)
- [`WiNDCHousehold.tax_rate`](@ref)
- [`WiNDCHousehold.duty_rate`](@ref)
"""
function armington_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = vcat(
        absorption(data; normalize = true),
        table(data, :Reexport, :National_Demand, :Local_Demand, :Import, :Margin_Demand; normalize = :Use),
        tax_rate(data),
        duty_rate(data)
    ) 

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    margin_supply_demand(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts margin supply and demand parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(data, :Margin_Demand, :Margin_Supply; normalize = :Use)
```
"""
function margin_supply_demand(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = table(data, :Margin_Demand, :Margin_Supply; normalize = :Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    consumption_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts consumption-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(data, :Personal_Consumption; normalize = :Use)
```
"""
function consumption_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = table(data, :Personal_Consumption; normalize = :Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    labor_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts leisure-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
   vcat(
        table(data, :Labor_Endowment),
        WiNDCHousehold.labor_supply(data),
        WiNDCHousehold.marginal_labor_tax_rate(data),
        WiNDCHousehold.fica_tax_rate(data)
    )
```

## Aggregate Data

- [`WiNDCHousehold.labor_supply`](@ref)
- [`WiNDCHousehold.marginal_labor_tax_rate`](@ref)
- [`WiNDCHousehold.fica_tax_rate`](@ref)
"""
function labor_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

   df = vcat(
        table(data, :Labor_Endowment),
        WiNDCHousehold.labor_supply(data),
        WiNDCHousehold.marginal_labor_tax_rate(data),
        WiNDCHousehold.fica_tax_rate(data)
    )


    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end


"""
    capital_stock_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts capital stock-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(data, :Capital_Demand; normalize = :Use)
```
"""
function capital_stock_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable

    df = table(data, :Capital_Demand; normalize = :Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
    
end

"""
    representative_agent_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts representative agent-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    vcat(
        leisure_consumption_elasticity(data),
        average_labor_tax_rate(data),
        labor_supply(data),
        leisure_demand(data),
        table(data, 
            :Personal_Consumption,
            :Household_Interest,
            :Transfer_Payment,
            :Savings,
            :Labor_Endowment;
            normalize=:Use
        )
    )
```

## Aggregate Data

- [`WiNDCHousehold.leisure_consumption_elasticity`](@ref)
- [`WiNDCHousehold.average_labor_tax_rate`](@ref)
- [`WiNDCHousehold.labor_supply`](@ref)
- [`WiNDCHousehold.leisure_demand`](@ref)
"""
function representative_agent_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable
    df = vcat(
        WiNDCHousehold.leisure_consumption_elasticity(data),
        WiNDCHousehold.average_labor_tax_rate(data),
        WiNDCHousehold.labor_supply(data),
        WiNDCHousehold.leisure_demand(data),
        table(data, 
            :Personal_Consumption,
            :Household_Interest,
            :Transfer_Payment,
            :Savings,
            :Labor_Endowment,
        
            normalize=:Use
            
        )
    )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end


"""
    NYSE_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts NYSE-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(data, 
        :Household_Supply,
        :Capital_Demand;
        normalize=:Use
    )
```
"""
function NYSE_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable
    df = table(data, 
        :Household_Supply,
        :Capital_Demand;
        normalize=:Use
    )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end

"""
    invest_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts investment-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(data, 
        :Investment_Final_Demand,
        :Savings;
        normalize=:Use
    )
```
"""
function invest_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable
    df = table(data, 
        :Investment_Final_Demand,
        :Savings;
        normalize=:Use
    )

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end

"""
    government_data(data::T, output = :DataFrame) where T<:AbstractHouseholdTable

Extracts government-related parameters from the regional data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    vcat(
        table(data, 
            :Government_Final_Demand,
            :Transfer_Payment,
            :Labor_Endowment;
            normalize=:Use
        ),
        WiNDCHousehold.government_deficit(data),
        WiNDCHousehold.average_labor_tax_rate(data),
    )
```

## Aggregate Data

- [`WiNDCHousehold.government_deficit`](@ref)
- [`WiNDCHousehold.average_labor_tax_rate`](@ref)
"""
function government_data(data::T; output = :DataFrame) where T<:AbstractHouseholdTable
    df = vcat(
        table(data, 
            :Government_Final_Demand,
            :Transfer_Payment,
            :Labor_Endowment;
            normalize=:Use
        ),
        WiNDCHousehold.government_deficit(data),
        WiNDCHousehold.average_labor_tax_rate(data),
    )


    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end

"""
    ssk_data(HH::HouseholdTable; output = :DataFrame)

Extracts steady-state capital stock-related parameters from the household data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(HH, :Investment_Final_Demand; normalize=:Use)
```
"""
function ssk_data(HH::HouseholdTable; output = :DataFrame)
    df = table(HH, :Investment_Final_Demand; normalize=:Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end

"""
    saverate_data(HH::HouseholdTable; output = :DataFrame)

Extracts saving rate-related parameters from the household data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(HH, :Investment_Final_Demand; normalize=:Use)
```
"""
function saverate_data(HH::HouseholdTable; output = :DataFrame)
    df = table(HH, :Investment_Final_Demand; normalize=:Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end

"""
    trans_data(HH::HouseholdTable; output = :DataFrame)

Extracts transfer-related parameters from the household data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(HH, :Government_Final_Demand; normalize=:Use)
```
"""
function trans_data(HH::HouseholdTable; output = :DataFrame)
    df = table(HH, :Government_Final_Demand; normalize=:Use)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:row], row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end

"""
    cpi_data(HH::HouseholdTable; output = :DataFrame)

Extracts consumer price index-related parameters from the household data table.

## Arguments

- `data::T`: A WiNDCHousehold data table.

## Keyword Arguments

- `output::Symbol = :DataFrame`: The output format, either `:DataFrame` or `:DefaultDict`.

## Returns

- A DataFrame or DefaultDict containing the parameter data.

## Data

```julia
    table(HH, :Personal_Consumption; normalize=:Use) |>
        x -> groupby(x, [:col, :region, :parameter]) |>
        x -> combine(x, :value => sum => :value)
```
"""
function cpi_data(HH::HouseholdTable; output = :DataFrame)
    df = table(HH, :Personal_Consumption; normalize=:Use) |>
        x -> groupby(x, [:col, :region, :parameter]) |>
        x -> combine(x, :value => sum => :value)

    if output == :DataFrame
        return df
    elseif output == :DefaultDict
        return DefaultDict(0,
            Dict((row[:col], row[:region], row[:parameter]) => row[:value] for row in eachrow(df))
        )
    else
        error("Unsupported output type: $output")
    end
end