package geomesa;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.geotools.data.*;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;
import util.Ini4j;
import org.locationtech.geomesa.index.conf.QueryHints;

public class Tutorial {
//    private final String simpleFeatureTypeName = "TestFeature";
    public Tutorial() {}

    public static DataStore createDataStore(Map<String, String> params) throws IOException {
        System.out.println("Loading datastore...");
        System.out.println(params);
        DataStore dataStore = DataStoreFinder.getDataStore(params);
        if (dataStore == null) {
            throw new RuntimeException("Could not create data store with provided parameters");
        }
        System.out.println("Datastore initialized!");
        return dataStore;
    }

    private static SimpleFeatureType createSimpleFeatureType(String simpleFeatureTypeName) {
        SimpleFeatureType sft;

        StringBuilder attributes = new StringBuilder();
        attributes.append("vendorID:String:index=true,");
        attributes.append("*geom:Polygon:srid=4326");

        sft = SimpleFeatureTypes.createType(simpleFeatureTypeName, attributes.toString());

//        sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
        return sft;
    }

    public static void createSchema(DataStore dataStore, String simpleFeatureTypeName) {
        try {
            SimpleFeatureType simpleFeatureType = createSimpleFeatureType(simpleFeatureTypeName);
            System.out.println("Creating schema: " + DataUtilities.encodeType(simpleFeatureType));
            dataStore.createSchema(simpleFeatureType);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("SFT created!");
    }

    public static void insertData(DataStore dataStore,String simpleFeatureTypeName) {
        System.out.println("Generating test data");
        try(FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                    dataStore.getFeatureWriterAppend(simpleFeatureTypeName, Transaction.AUTO_COMMIT)) {
            SimpleFeature newFeature = writer.next();
            newFeature.setAttribute("geom", "POLYGON ((10 10,10 20,20 20,20 10,10 10))");
            newFeature.setAttribute("vendorID", "0001");
            writer.write();
            newFeature = writer.next();
            newFeature.setAttribute("geom", "POLYGON ((10 10,20 10,20 20,10 20,10 10))");
            newFeature.setAttribute("vendorID", "0002");
            writer.write();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Write success");
    }

    public static List<Query> getTestQueries(String simpleFeatureTypeName,String bbox, String during) {
        try {
            List<Query> queries = new ArrayList<>();
            // basic spatial query
            Query q1 = new Query(simpleFeatureTypeName, ECQL.toFilter(bbox));
//            q1.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);
//            q1.getHints().put(QueryHints.EXACT_COUNT(), Boolean.TRUE);
            queries.add(q1);
            // basic spatio-temporal query
//            Query q2 = new Query(simpleFeatureTypeName, ECQL.toFilter(bbox + " AND " + during));
//            q2.getHints().put(QueryHints.LOOSE_BBOX(), Boolean.FALSE);
//            q2.getHints().put(QueryHints.EXACT_COUNT(), Boolean.TRUE);
//            queries.add(q2);
            // basic spatial query with projection down to a few attributes
//            queries.add(new Query(simpleFeatureTypeName, ECQL.toFilter(bbox),new String[]{"taxiId"}));
            // attribute query on a secondary index - note we specified index=true for EventCode
//            queries.add(new Query(simpleFeatureTypeName, ECQL.toFilter("EventCode = '051'")));
            // attribute query on a secondary index with a projection
//            queries.add(new Query(simpleFeatureTypeName, ECQL.toFilter("EventCode = '051' AND " + during),
//                    new String[]{"taxiId", "dtg"}));
            return queries;
        }
        catch (CQLException e) {
            throw new RuntimeException("Error creating filter:", e);
        }
    }

    public static void queryFeatures(DataStore dataStore,List<Query> queries) throws IOException {
        for (Query query : queries) {
            System.out.println("Running query " + ECQL.toCQL(query.getFilter()));
            if (query.getPropertyNames() != null) {
                System.out.println("Returning attributes " + Arrays.asList(query.getPropertyNames()));
            }
            if (query.getSortBy() != null) {
                SortBy sort = query.getSortBy()[0];
                System.out.println("Sorting by " + sort.getPropertyName() + " " + sort.getSortOrder());
            }
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                         dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT)) {
                // loop through all results, only print out the first 10
                int n = 0;
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    if (n++ < 10) {
                        // use geotools data utilities to get a printable string
                        System.out.println(String.format("%02d", n) + " " + DataUtilities.encodeFeature(feature));
                    } else if (n == 10) {
                        System.out.println("...");
                    }
                }
                System.out.println();
                System.out.println("Returned " + n + " total features");
                System.out.println();
            }
        }
    }

    public static void main(String[] args) throws IOException, CQLException {
        Map<String, String> params = new HashMap<>();
        params.put("hbase.catalog", args[0]);
        String simpleFeatureTypeName = args[1];
        DataStore dataStore = createDataStore(params);

        //读取conf文件配置参数
//        Ini4j ini4j=new Ini4j( "geomesaQuery.conf");
//        double minLat = Double.parseDouble( ini4j.readIni( "RangeQueryCondition" , "minLat") );
//        double maxLat = Double.parseDouble( ini4j.readIni( "RangeQueryCondition" , "maxLat") );
//        double minLng = Double.parseDouble( ini4j.readIni( "RangeQueryCondition" , "minLng") );
//        double maxLng = Double.parseDouble( ini4j.readIni( "RangeQueryCondition" , "maxLng") );
//        String timeFrom=String.valueOf(ini4j.readIni( "RangeQueryCondition" , "startTime"));
//        String timeTo=String.valueOf(ini4j.readIni( "RangeQueryCondition" , "endTime"));
//        String bbox = "BBOX(geom,"+minLng+","+minLat+","+maxLng+","+maxLat+")";
//        String during = "dtg DURING "+ timeFrom +".000Z/"+timeTo+".000Z";

//        createSchema(dataStore,simpleFeatureTypeName);
//        insertData(dataStore,simpleFeatureTypeName);
//        queryFeatures(dataStore,getTestQueries(simpleFeatureTypeName,bbox,during));



        Random r = new Random();
        double scale = Double.parseDouble(args[2]);
        long t1 = System.currentTimeMillis();
        for(int i = 0; i < 15;i++) {
            long temp = System.currentTimeMillis();
//            double minLat = 40.73 + (40.9 - 40.73) * r.nextDouble();
//            double minLng = -73.9 + (-73 - (-73.9)) * r.nextDouble();
            double minLat = 40.5 + (41 - 40.5) * r.nextDouble();
            double minLng = -74.5 + (-73.8 - (-74.5)) * r.nextDouble();
            double maxLat = minLat + scale;
            double maxLng = minLng + scale;
            String bbox = "BBOX(geom,"+minLng+","+minLat+","+maxLng+","+maxLat+")";
            queryFeatures(dataStore,getTestQueries(simpleFeatureTypeName,bbox,""));
            System.out.println("\ttime:"+(System.currentTimeMillis()-temp));
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Ave spend time: " + (t2 - t1)/15 + "ms");
    }
}
