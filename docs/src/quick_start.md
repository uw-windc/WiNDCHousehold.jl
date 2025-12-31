# Quick Start

This guide provides a brief overview of how to use the WiNDCHousehold package.Follow the steps below to get started quickly.


## Set up Julia Environment

It is recommended to set up a Julia environment when ever you begin a new project. You can do this by creating a new directory, starting Julia, and activating it in Julia:

```julia
julia> ]
pkg> activate .
```

Then we install the necessary packages:

```julia
pkg> add WiNDCHousehold, JLD2, DataFrames
```

The `JLD2` package isn't strictly necessary, we will use it to load the disaggregated Household data. The data can also be but from source using the directions in the [Disaggregation Data](#dissaggregation-data) section.

## Perform the Disaggregation

We are going to load pre-disaggregated data, download the JLD2 file [household.jld2](https://drive.google.com/file/d/1zec5_SoX7QIQNlLYW3Rb13UAw8rnFLFo/view?usp=sharing)`. 


```julia
using WiNDCHousehold, JLD2, DataFrames
using WiNDCHousehold.WiNDCContainer

@load "household.jld2" HH
```

This loads the Household table as `HH`. This, in essence, stores the data in a DataFrame. You can view the data using:

```julia
table(HH)
```

This DataFrame has 4 domain columns (`row`, `col`, `region`, `year`), a column indicating the parameter, and the values. The entries in each domain column are `elements` of `sets`. You can view these using:

```julia
sets(HH)
elements(HH)
```

To view elements for a speicific set (or sets), add them as symbol arguments:

```julia
elements(HH, :region)
elements(HH, :region, :sector)
```
More information on the sets can be found [in the Sets section](#household-data-sets).

Parameters can be extracted from the table using the symbol arguments:

```julia
table(HH, :Intermediate_Demand, :Intermediate_Supply)

table(HH, :sector, :Value_Added)
```


## Verify Results in CGE Model

To ensure the disaggregation process was successful, we perform a benchmark verification using a CGE model.

```julia
M = household_model(HH);
solve!(M, cumulative_iteration_limit = 0)
```

This should give a very small residual, indicating that the disaggregation was successful and the data is balanced. The full model documentation is available in the [Household Model section](#household-model).

If you receive strange errors during the solve (like `NaN` values), it is likely that you lack a PATHSolver license. Refer to the [PATHSolver.jl](https://github.com/chkwon/PATHSolver.jl#license) documentation for more information on obtaining a license.