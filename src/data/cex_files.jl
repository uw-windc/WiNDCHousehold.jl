function load_cex_income_elasticities(path::String)

    return CSV.read(path, DataFrame) |>
        x -> select(x, :cex, :elast)
end


function load_pce_shares(
    path::String;
    naics_windc_map = load_windc_naics_map()
)

    return CSV.read(path, DataFrame) |>
        x -> select(x, :Column1 => :cex, :Column2 => :windc, :pct_windc => :value) |>
        x -> innerjoin(x, naics_windc_map, on = :windc) |>
        x -> select(x, :cex, :naics, :value) |>
        x -> transform(x,
            :value => ByRow(y -> y/100) => :value
        )

end