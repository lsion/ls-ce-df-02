MAIN_CLASS=org.lsion.ce.df.CSV2BTSegmentDFCloud
PROJECT_ID=lsion-151311
STAGING_BUCKET=gs://ls-ce/staging
OUTPUT_BUCKET=gs://ls-ce/output
TEMPLATES_BUCKET=gs://ls-ce/templates/CSV2BT

mvn compile exec:java \
     -Dexec.mainClass=$MAIN_CLASS \
     -Dexec.args="--runner=DataflowRunner \
                  --project=$PROJECT_ID \
                  --stagingLocation=$STAGING_BUCKET \
                  --templateLocation=$TEMPLATES_BUCKET"
