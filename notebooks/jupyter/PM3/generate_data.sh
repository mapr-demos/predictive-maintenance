#!/bin/bash

# Generate 10 good/degraded/failure datasets for training
# "stage 1" is what I call the state of a machine in good condition
# "stage 2" is what I call the state of a machine in degrades, but still functional, condition
# "stage 3" is what I call the state of a machine that is malfunctioning
# We combine the data for all three stages to create a single picture of a machine that goes from performing well, to degraded, to failure.  The idea here is that we get enough of these pictures (e.g. 500 of them) then we will be able to train an ML model to predict that a machine is near failure when it's operating in what we have defined as the degraded state.
NUM_DATASETS=19

rm -f *stage* *train* *test*

for i in `seq 0 $NUM_DATASETS`; do 
stage1_length=$((450 + $RANDOM % 100));
stage2_length=$((100 + $RANDOM % 10));
stage3_length=$((50 + $RANDOM % 10));
~/development/log-synth/target/log-synth -count $stage1_length -schema PM_schema1.json -format JSON  > $i-stage1.json;
sed -i .bak "s/\"start\": [0-9]*/\"start\": $stage1_length/g" PM_schema2.json
~/development/log-synth/target/log-synth -count $stage2_length -schema PM_schema2.json -format JSON  > $i-stage2.json; 
sed -i .bak "s/\"start\": [0-9]*/\"start\": $(($stage1_length+$stage2_length))/g" PM_schema3.json
~/development/log-synth/target/log-synth -count $stage3_length -schema PM_schema3.json -format JSON  > $i-stage3.json; 
done

for i in `seq 0 $NUM_DATASETS`; do 
cat $i-stage1.json $i-stage2.json >> $i-tmp.json; 
max=`wc -l $i-tmp.json | awk '{print $1}'`
cat $i-tmp.json $i*stage3.json | sed "s/}/,\"max_life\":$max}/g" > $i-train.json; 
rm $i-tmp.json; 
done

cat *-train.json >> PM_logsynth_train.json

# Generate datasets for testing

stage1_length=500;
stage2_length=50;
stage3_length=50;
~/development/log-synth/target/log-synth -count $stage1_length -schema PM_schema1.json -format JSON  > test_stage1.json;
sed -i .bak "s/\"start\": [0-9]*/\"start\": $stage1_length/g" PM_schema2.json
~/development/log-synth/target/log-synth -count $stage2_length -schema PM_schema2.json -format JSON  > test_stage2.json; 
sed -i .bak "s/\"start\": [0-9]*/\"start\": $(($stage1_length+$stage2_length))/g" PM_schema3.json
~/development/log-synth/target/log-synth -count $stage3_length -schema PM_schema3.json -format JSON  > test_stage3.json; 
cat test_stage1.json test_stage2.json >> test_tmp.json; max=`wc -l test_tmp.json | awk '{print $1}'`; cat test_tmp.json test_stage3.json | sed "s/}/,\"max_life\":$max}/g" > PM_logsynth_test1.json; rm test_tmp.json

stage1_length=600;
~/development/log-synth/target/log-synth -count $stage1_length -schema PM_schema0.json -format JSON  > test_stage1.json;
cat test_stage1.json >> test_tmp.json; max=`wc -l test_tmp.json | awk '{print $1}'`; cat test_tmp.json | sed "s/}/,\"max_life\":$max}/g" > PM_logsynth_test2.json; rm test_tmp.json

stage1_length=200;
stage2_length=200;
stage3_length=200;
~/development/log-synth/target/log-synth -count $stage1_length -schema PM_schema1.json -format JSON  > test_stage1.json;
sed -i .bak "s/\"start\": [0-9]*/\"start\": $stage1_length/g" PM_schema2.json
~/development/log-synth/target/log-synth -count $stage2_length -schema PM_schema2.json -format JSON  > test_stage2.json; 
sed -i .bak "s/\"start\": [0-9]*/\"start\": $(($stage1_length+$stage2_length))/g" PM_schema3.json
~/development/log-synth/target/log-synth -count $stage3_length -schema PM_schema3.json -format JSON  > test_stage3.json; 
cat test_stage1.json test_stage2.json >> test_tmp.json; max=`wc -l test_tmp.json | awk '{print $1}'`; cat test_tmp.json test_stage3.json | sed "s/}/,\"max_life\":$max}/g" > PM_logsynth_test3.json; rm test_tmp.json

stage1_length=50;
stage2_length=50;
stage3_length=500;
~/development/log-synth/target/log-synth -count $stage1_length -schema PM_schema1.json -format JSON  > test_stage1.json;
sed -i .bak "s/\"start\": [0-9]*/\"start\": $stage1_length/g" PM_schema2.json
~/development/log-synth/target/log-synth -count $stage2_length -schema PM_schema2.json -format JSON  > test_stage2.json; 
sed -i .bak "s/\"start\": [0-9]*/\"start\": $(($stage1_length+$stage2_length))/g" PM_schema3.json
~/development/log-synth/target/log-synth -count $stage3_length -schema PM_schema3.json -format JSON  > test_stage3.json; 
cat test_stage1.json test_stage2.json >> test_tmp.json; max=`wc -l test_tmp.json | awk '{print $1}'`; cat test_tmp.json test_stage3.json | sed "s/}/,\"max_life\":$max}/g" > PM_logsynth_test4.json; rm test_tmp.json
