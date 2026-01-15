module WiNDCHousehold

    using DataFrames, CSV, XLSX, WiNDCContainer, WiNDCRegional, HTTP, JSON
    using Downloads, ZipFile

    import DataStructures: DefaultDict
    using MPSGE, YAML

    import WiNDCContainer: domain, base_table, sets, elements

    using JuMP, Ipopt, JLD2

    

    include("structs.jl")
    export HouseholdTable

    include("maps.jl")

    include("data/magic_numbers.jl")

    include("data/cps.jl")
    export load_cps_data_api

    include("data/get_nipa.jl")
    export load_nipa_data_api

    include("data/get_acs.jl")
    export load_acs_data_api

    include("data/medicare.jl")

    include("data/tax_rates.jl")

    include("data/cex_files.jl")

    include("data/pre_calibration_data.jl")

    include("data/calibration_model_1.jl")
    include("data/calibration_model_2.jl")

    include("data/load_raw_data.jl")


    include("aggregate_parameters.jl")
    export leisure_supply

    include("build.jl")

    include("model.jl")
    export household_model




end # module WiNDCHousehold
