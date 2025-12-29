function cbo_wealth_distribution()
    return DataFrame([
        (hh = :hh1, value = 0.025871517,)
        (hh = :hh2, value = 0.043989237,)
        (hh = :hh3, value = 0.077542098,)
        (hh = :hh4, value = 0.147248546,)
        (hh = :hh5, value = 0.705348602,)
    ])
end

function bls_distribution_expenditures(raw_data::WiNDCHousehold.RawHouseholdData)
    df = DataFrame([
            (hh = :hh1, value = 25138),
            (hh = :hh2, value = 36770),
            (hh = :hh3, value = 47664),
            (hh = :hh4, value = 64910),
            (hh = :hh5, value = 112221),
        ]) |>
        x -> outerjoin(
            x,

            raw_data.numhh |>
                x -> transform(x,
                    :hh => ByRow(Symbol) => :hh
                ),

            on = [:hh]
        ) |>
        x -> transform(x,
            [:value, :numhh] => ByRow(*) => :bls
        ) |>
        x -> select(x, :hh, :state, :bls) |>
        x -> groupby(x, :state) |>
        x -> combine(x, 
            :hh => identity => :hh,
            :bls => (y -> y/sum(y)) => :bls,
        )


    return df
end