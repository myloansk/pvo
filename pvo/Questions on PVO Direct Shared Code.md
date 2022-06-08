### Questions on PVO Direct Shared Code

#### General
    Error: data was not loaded to pyspark.DataFrame due to the fact that filepaths were not found
    Question: Are all filepath updated 

#### Script:SEGMEX2_PVO_300_CA_ML
    Reference:Constructor of Modelling  Line:2-22
    Error: Parsed Config in the master notebook did not corresponding to arguments defined in class constructor
    Description: In master notebook, to instantiate Modelling class, the following argument is parser to constuctor 
                train_new_model = ml_config.get("TRAIN NEW MODEL"),
                this argument was not reconginsed and throwed an error, however when we decided to omit it completely it worked
    Question: Is the case that newest version of Modelling was pushed without updating master notebook Modelling class invokation

#### Script:SEGMEX2_PVO_301_CA_POTENTIAL_ESTIMATION
    Reference:Method get_benchmark_values_ca  Line:427 
    Error: AssertionError
    Question: Is it complete necessary to get through those assertions ?

#### Script:SEGMEX2_PVO_302_CA_SIMULATION
    Reference:Method:potential_calc_simulation, Line:142
    Error: Couldn't track column WEEKLY_POTENTIAL
    Description: The output column is used as an input to create itself
    Question: Should we change column WEEKLY_POTENTIAL to WEEKLY_POTENTIAL_VALUE, WEEKLY_POTENTIAL_VOLUME
              instead ? Please specify if other option