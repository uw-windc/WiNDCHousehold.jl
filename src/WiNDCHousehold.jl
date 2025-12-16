module WiNDCHousehold

    using DataFrames, CSV, XLSX, WiNDCContainer, WiNDCRegional, HTTP, JSON
    using Downloads, ZipFile

    import DataStructures: DefaultDict
    using MPSGE, YAML

    import WiNDCContainer: domain, base_table, sets, elements

    include("structs.jl")
    export HouseholdTable

    include("maps.jl")

    include("data/cps.jl")
    export load_cps_data_api

    include("data/get_nipa.jl")
    export load_nipa_data_api

    include("data/get_acs.jl")
    export load_acs_data_api

    include("data/medicare.jl")

    include("build.jl")

    




end # module WiNDCHousehold
