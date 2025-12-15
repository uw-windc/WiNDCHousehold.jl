module WiNDCHousehold

    using DataFrames, CSV, XLSX, WiNDCContainer

    import DataStructures: DefaultDict
    using MPSGE, YAML

    import WiNDCContainer: domain, base_table, sets, elements

    include("structs.jl")
    export HouseholdTable

end # module WiNDCHousehold
