package es.bsc.aeneas.fastcsvloader;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.google.common.util.concurrent.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This program uses map/reduce to just run a distributed job where there is
 * no interaction between the tasks and each task writes a large unsorted
 * random sequence of words.
 * In order for this program to generate data for terasort with a 5-10 words
 * per key and 20-100 words per value, have the following config:
 * <xmp>
 * <?xml version="1.0"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *   <property>
 *     <name>mapreduce.randomtextwriter.minwordskey</name>
 *     <value>5</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.maxwordskey</name>
 *     <value>10</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.minwordsvalue</name>
 *     <value>20</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.maxwordsvalue</name>
 *     <value>100</value>
 *   </property>
 *   <property>
 *     <name>mapreduce.randomtextwriter.totalbytes</name>
 *     <value>1099511627776</value>
 *   </property>
 * </configuration></xmp>
 *
 * Equivalently, {@link RandomTextWriter} also supports all the above options
 * and ones supported by {@link Tool} via the command-line.
 *
 * To run: bin/hadoop jar hadoop-${version}-examples.jar randomtextwriter
 *            [-outFormat <i>output format class</i>] <i>output</i> 
 */
public class RandomTextWriter  {
    private final static Logger log= LoggerFactory.getLogger(RandomTextWriter.class);
    public static final String TOTAL_BYTES =
            "mapreduce.randomtextwriter.totalbytes";

    public static final String MAX_VALUE = "mapreduce.randomtextwriter.maxwordsvalue";
    public static final String MIN_VALUE = "mapreduce.randomtextwriter.minwordsvalue";
    public static final String MIN_KEY = "mapreduce.randomtextwriter.minwordskey";
    public static final String MAX_KEY = "mapreduce.randomtextwriter.maxwordskey";



    /**
     * User counters
     */
    static class RandomTextMapper implements Callable<Integer> {

        private final int id;
        private final int threads;
        private long numBytesToWrite;
        private int minWordsInKey;
        private int wordsInKeyRange;
        private int minWordsInValue;
        private int wordsInValueRange;
        private Random random = new Random();
        private final  CqlFrameLoader cqlLoader;

        RandomTextMapper(String query,int id, int threads,long numBytesToWrite ){
            this.numBytesToWrite=numBytesToWrite;
            this.id = id;
            this.threads = threads;
            cqlLoader = new CqlFrameLoader(query);
              minWordsInKey = Integer.getInteger(MIN_KEY, 5);
            wordsInKeyRange = (Integer.getInteger(MAX_KEY, 10) - minWordsInKey);
            minWordsInValue = Integer.getInteger(MIN_VALUE, 10);
            wordsInValueRange = (Integer.getInteger(MAX_VALUE, 100) - minWordsInValue);
        }



        /**
         * Given an output filename, write a bunch of random records to it.
         */
        public Integer call () throws Exception {
            int itemCount = 0;
            while (numBytesToWrite > 0) {
                // Generate the key/value
                int noWordsKey = minWordsInKey +
                        (wordsInKeyRange != 0 ? random.nextInt(wordsInKeyRange) : 0);
                int noWordsValue = minWordsInValue +
                        (wordsInValueRange != 0 ? random.nextInt(wordsInValueRange) : 0);
                Text keyWords = generateSentence(noWordsKey);
                Text valueWords = generateSentence(noWordsValue);

                // Write the sentence
                BatchStatement bs=new BatchStatement();
                for(String word :(keyWords+" "+valueWords).split(" ")){
                    int position=++itemCount*threads+id;
                    cqlLoader.addToBatch(bs,new String[]{word,position+"",word});
                }

                boolean done=false;
                int retry=10;
                int sleepFor=1000; //seconds
                while(!done) {
                    try {
                        cqlLoader.insertBatch(bs);
                        done=true;
                    } catch (WriteTimeoutException wte){
                        log.warn("Got timeout : retry {}, sleeping for {}",retry,sleepFor);
                        sleepFor*=2;
                        if(retry--==0)
                            throw wte;

                    }

                }

                numBytesToWrite -= (keyWords.getLength() + valueWords.getLength());

                if (itemCount % 200 == 0) {
                    log.info("Thread#{} : wrote record {}. {}"
                            + " bytes left.", id, itemCount, numBytesToWrite);
                }
            }
            log.info("done with " + itemCount + " records.");
            return itemCount;
        }

        private Text generateSentence(int noWords) {
            StringBuffer sentence = new StringBuffer();
            String space = " ";
            for (int i=0; i < noWords; ++i) {
                sentence.append(words[random.nextInt(words.length)]);
                sentence.append(space);
            }
            return new Text(sentence.toString());
        }
    }



    /**
     * This is the main routine for launching a distributed random write job.
     * It runs 10 maps/node and each node writes 1 gig of data to a DFS file.
     * The reduce doesn't do anything.
     *
     * @throws IOException
     */
    public static void main(String[] args) throws Exception {

        String query=checkNotNull(System.getProperty("query"),"provide the paramenter query: (-Dquery=\"\")");
        long totalBytesToWrite = checkNotNull(Long.getLong(TOTAL_BYTES),"Please, provide "+TOTAL_BYTES);

        int numThreads = Integer.getInteger("numThreads",16);


        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
            private int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                log.info("Created thread {}", i);
                return new Thread(r, "DisThread-" + i++); //Just to track each thread

            }

        }));

        // Specify the size of the ring buffer, must be power of 2.

        final CountDownLatch latch = new CountDownLatch(numThreads);

        log.info("Using {} concurrent inserts", numThreads);
        long time = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            ListenableFuture<Integer> future = executor.submit(new RandomTextMapper(query,i,numThreads,totalBytesToWrite/numThreads));

            Futures.addCallback(future, new FutureCallback<Integer>() {
                @Override
                public void onSuccess(Integer result) {
                    log.info("Thread completed inserting {} lines", result);
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    latch.countDown();
                    t.printStackTrace();
                    log.error("Error in one inserting thread. Killing everything");
                    System.exit(-1);
                }
            });
        }


        latch.await();
        log.info("Load completed of {} bytes in {} ms", totalBytesToWrite, System.currentTimeMillis() - time);
        executor.shutdown();



    }



    /**
     * A random list of 1000 words from /usr/share/dict/words
     */
    private static String[] words = {
            "diurnalness", "Homoiousian",
            "spiranthic", "tetragynian",
            "silverhead", "ungreat",
            "lithograph", "exploiter",
            "physiologian", "by",
            "hellbender", "Filipendula",
            "undeterring", "antiscolic",
            "pentagamist", "hypoid",
            "cacuminal", "sertularian",
            "schoolmasterism", "nonuple",
            "gallybeggar", "phytonic",
            "swearingly", "nebular",
            "Confervales", "thermochemically",
            "characinoid", "cocksuredom",
            "fallacious", "feasibleness",
            "debromination", "playfellowship",
            "tramplike", "testa",
            "participatingly", "unaccessible",
            "bromate", "experientialist",
            "roughcast", "docimastical",
            "choralcelo", "blightbird",
            "peptonate", "sombreroed",
            "unschematized", "antiabolitionist",
            "besagne", "mastication",
            "bromic", "sviatonosite",
            "cattimandoo", "metaphrastical",
            "endotheliomyoma", "hysterolysis",
            "unfulminated", "Hester",
            "oblongly", "blurredness",
            "authorling", "chasmy",
            "Scorpaenidae", "toxihaemia",
            "Dictograph", "Quakerishly",
            "deaf", "timbermonger",
            "strammel", "Thraupidae",
            "seditious", "plerome",
            "Arneb", "eristically",
            "serpentinic", "glaumrie",
            "socioromantic", "apocalypst",
            "tartrous", "Bassaris",
            "angiolymphoma", "horsefly",
            "kenno", "astronomize",
            "euphemious", "arsenide",
            "untongued", "parabolicness",
            "uvanite", "helpless",
            "gemmeous", "stormy",
            "templar", "erythrodextrin",
            "comism", "interfraternal",
            "preparative", "parastas",
            "frontoorbital", "Ophiosaurus",
            "diopside", "serosanguineous",
            "ununiformly", "karyological",
            "collegian", "allotropic",
            "depravity", "amylogenesis",
            "reformatory", "epidymides",
            "pleurotropous", "trillium",
            "dastardliness", "coadvice",
            "embryotic", "benthonic",
            "pomiferous", "figureheadship",
            "Megaluridae", "Harpa",
            "frenal", "commotion",
            "abthainry", "cobeliever",
            "manilla", "spiciferous",
            "nativeness", "obispo",
            "monilioid", "biopsic",
            "valvula", "enterostomy",
            "planosubulate", "pterostigma",
            "lifter", "triradiated",
            "venialness", "tum",
            "archistome", "tautness",
            "unswanlike", "antivenin",
            "Lentibulariaceae", "Triphora",
            "angiopathy", "anta",
            "Dawsonia", "becomma",
            "Yannigan", "winterproof",
            "antalgol", "harr",
            "underogating", "ineunt",
            "cornberry", "flippantness",
            "scyphostoma", "approbation",
            "Ghent", "Macraucheniidae",
            "scabbiness", "unanatomized",
            "photoelasticity", "eurythermal",
            "enation", "prepavement",
            "flushgate", "subsequentially",
            "Edo", "antihero",
            "Isokontae", "unforkedness",
            "porriginous", "daytime",
            "nonexecutive", "trisilicic",
            "morphiomania", "paranephros",
            "botchedly", "impugnation",
            "Dodecatheon", "obolus",
            "unburnt", "provedore",
            "Aktistetae", "superindifference",
            "Alethea", "Joachimite",
            "cyanophilous", "chorograph",
            "brooky", "figured",
            "periclitation", "quintette",
            "hondo", "ornithodelphous",
            "unefficient", "pondside",
            "bogydom", "laurinoxylon",
            "Shiah", "unharmed",
            "cartful", "noncrystallized",
            "abusiveness", "cromlech",
            "japanned", "rizzomed",
            "underskin", "adscendent",
            "allectory", "gelatinousness",
            "volcano", "uncompromisingly",
            "cubit", "idiotize",
            "unfurbelowed", "undinted",
            "magnetooptics", "Savitar",
            "diwata", "ramosopalmate",
            "Pishquow", "tomorn",
            "apopenptic", "Haversian",
            "Hysterocarpus", "ten",
            "outhue", "Bertat",
            "mechanist", "asparaginic",
            "velaric", "tonsure",
            "bubble", "Pyrales",
            "regardful", "glyphography",
            "calabazilla", "shellworker",
            "stradametrical", "havoc",
            "theologicopolitical", "sawdust",
            "diatomaceous", "jajman",
            "temporomastoid", "Serrifera",
            "Ochnaceae", "aspersor",
            "trailmaking", "Bishareen",
            "digitule", "octogynous",
            "epididymitis", "smokefarthings",
            "bacillite", "overcrown",
            "mangonism", "sirrah",
            "undecorated", "psychofugal",
            "bismuthiferous", "rechar",
            "Lemuridae", "frameable",
            "thiodiazole", "Scanic",
            "sportswomanship", "interruptedness",
            "admissory", "osteopaedion",
            "tingly", "tomorrowness",
            "ethnocracy", "trabecular",
            "vitally", "fossilism",
            "adz", "metopon",
            "prefatorial", "expiscate",
            "diathermacy", "chronist",
            "nigh", "generalizable",
            "hysterogen", "aurothiosulphuric",
            "whitlowwort", "downthrust",
            "Protestantize", "monander",
            "Itea", "chronographic",
            "silicize", "Dunlop",
            "eer", "componental",
            "spot", "pamphlet",
            "antineuritic", "paradisean",
            "interruptor", "debellator",
            "overcultured", "Florissant",
            "hyocholic", "pneumatotherapy",
            "tailoress", "rave",
            "unpeople", "Sebastian",
            "thermanesthesia", "Coniferae",
            "swacking", "posterishness",
            "ethmopalatal", "whittle",
            "analgize", "scabbardless",
            "naught", "symbiogenetically",
            "trip", "parodist",
            "columniform", "trunnel",
            "yawler", "goodwill",
            "pseudohalogen", "swangy",
            "cervisial", "mediateness",
            "genii", "imprescribable",
            "pony", "consumptional",
            "carposporangial", "poleax",
            "bestill", "subfebrile",
            "sapphiric", "arrowworm",
            "qualminess", "ultraobscure",
            "thorite", "Fouquieria",
            "Bermudian", "prescriber",
            "elemicin", "warlike",
            "semiangle", "rotular",
            "misthread", "returnability",
            "seraphism", "precostal",
            "quarried", "Babylonism",
            "sangaree", "seelful",
            "placatory", "pachydermous",
            "bozal", "galbulus",
            "spermaphyte", "cumbrousness",
            "pope", "signifier",
            "Endomycetaceae", "shallowish",
            "sequacity", "periarthritis",
            "bathysphere", "pentosuria",
            "Dadaism", "spookdom",
            "Consolamentum", "afterpressure",
            "mutter", "louse",
            "ovoviviparous", "corbel",
            "metastoma", "biventer",
            "Hydrangea", "hogmace",
            "seizing", "nonsuppressed",
            "oratorize", "uncarefully",
            "benzothiofuran", "penult",
            "balanocele", "macropterous",
            "dishpan", "marten",
            "absvolt", "jirble",
            "parmelioid", "airfreighter",
            "acocotl", "archesporial",
            "hypoplastral", "preoral",
            "quailberry", "cinque",
            "terrestrially", "stroking",
            "limpet", "moodishness",
            "canicule", "archididascalian",
            "pompiloid", "overstaid",
            "introducer", "Italical",
            "Christianopaganism", "prescriptible",
            "subofficer", "danseuse",
            "cloy", "saguran",
            "frictionlessly", "deindividualization",
            "Bulanda", "ventricous",
            "subfoliar", "basto",
            "scapuloradial", "suspend",
            "stiffish", "Sphenodontidae",
            "eternal", "verbid",
            "mammonish", "upcushion",
            "barkometer", "concretion",
            "preagitate", "incomprehensible",
            "tristich", "visceral",
            "hemimelus", "patroller",
            "stentorophonic", "pinulus",
            "kerykeion", "brutism",
            "monstership", "merciful",
            "overinstruct", "defensibly",
            "bettermost", "splenauxe",
            "Mormyrus", "unreprimanded",
            "taver", "ell",
            "proacquittal", "infestation",
            "overwoven", "Lincolnlike",
            "chacona", "Tamil",
            "classificational", "lebensraum",
            "reeveland", "intuition",
            "Whilkut", "focaloid",
            "Eleusinian", "micromembrane",
            "byroad", "nonrepetition",
            "bacterioblast", "brag",
            "ribaldrous", "phytoma",
            "counteralliance", "pelvimetry",
            "pelf", "relaster",
            "thermoresistant", "aneurism",
            "molossic", "euphonym",
            "upswell", "ladhood",
            "phallaceous", "inertly",
            "gunshop", "stereotypography",
            "laryngic", "refasten",
            "twinling", "oflete",
            "hepatorrhaphy", "electrotechnics",
            "cockal", "guitarist",
            "topsail", "Cimmerianism",
            "larklike", "Llandovery",
            "pyrocatechol", "immatchable",
            "chooser", "metrocratic",
            "craglike", "quadrennial",
            "nonpoisonous", "undercolored",
            "knob", "ultratense",
            "balladmonger", "slait",
            "sialadenitis", "bucketer",
            "magnificently", "unstipulated",
            "unscourged", "unsupercilious",
            "packsack", "pansophism",
            "soorkee", "percent",
            "subirrigate", "champer",
            "metapolitics", "spherulitic",
            "involatile", "metaphonical",
            "stachyuraceous", "speckedness",
            "bespin", "proboscidiform",
            "gul", "squit",
            "yeelaman", "peristeropode",
            "opacousness", "shibuichi",
            "retinize", "yote",
            "misexposition", "devilwise",
            "pumpkinification", "vinny",
            "bonze", "glossing",
            "decardinalize", "transcortical",
            "serphoid", "deepmost",
            "guanajuatite", "wemless",
            "arval", "lammy",
            "Effie", "Saponaria",
            "tetrahedral", "prolificy",
            "excerpt", "dunkadoo",
            "Spencerism", "insatiately",
            "Gilaki", "oratorship",
            "arduousness", "unbashfulness",
            "Pithecolobium", "unisexuality",
            "veterinarian", "detractive",
            "liquidity", "acidophile",
            "proauction", "sural",
            "totaquina", "Vichyite",
            "uninhabitedness", "allegedly",
            "Gothish", "manny",
            "Inger", "flutist",
            "ticktick", "Ludgatian",
            "homotransplant", "orthopedical",
            "diminutively", "monogoneutic",
            "Kenipsim", "sarcologist",
            "drome", "stronghearted",
            "Fameuse", "Swaziland",
            "alen", "chilblain",
            "beatable", "agglomeratic",
            "constitutor", "tendomucoid",
            "porencephalous", "arteriasis",
            "boser", "tantivy",
            "rede", "lineamental",
            "uncontradictableness", "homeotypical",
            "masa", "folious",
            "dosseret", "neurodegenerative",
            "subtransverse", "Chiasmodontidae",
            "palaeotheriodont", "unstressedly",
            "chalcites", "piquantness",
            "lampyrine", "Aplacentalia",
            "projecting", "elastivity",
            "isopelletierin", "bladderwort",
            "strander", "almud",
            "iniquitously", "theologal",
            "bugre", "chargeably",
            "imperceptivity", "meriquinoidal",
            "mesophyte", "divinator",
            "perfunctory", "counterappellant",
            "synovial", "charioteer",
            "crystallographical", "comprovincial",
            "infrastapedial", "pleasurehood",
            "inventurous", "ultrasystematic",
            "subangulated", "supraoesophageal",
            "Vaishnavism", "transude",
            "chrysochrous", "ungrave",
            "reconciliable", "uninterpleaded",
            "erlking", "wherefrom",
            "aprosopia", "antiadiaphorist",
            "metoxazine", "incalculable",
            "umbellic", "predebit",
            "foursquare", "unimmortal",
            "nonmanufacture", "slangy",
            "predisputant", "familist",
            "preaffiliate", "friarhood",
            "corelysis", "zoonitic",
            "halloo", "paunchy",
            "neuromimesis", "aconitine",
            "hackneyed", "unfeeble",
            "cubby", "autoschediastical",
            "naprapath", "lyrebird",
            "inexistency", "leucophoenicite",
            "ferrogoslarite", "reperuse",
            "uncombable", "tambo",
            "propodiale", "diplomatize",
            "Russifier", "clanned",
            "corona", "michigan",
            "nonutilitarian", "transcorporeal",
            "bought", "Cercosporella",
            "stapedius", "glandularly",
            "pictorially", "weism",
            "disilane", "rainproof",
            "Caphtor", "scrubbed",
            "oinomancy", "pseudoxanthine",
            "nonlustrous", "redesertion",
            "Oryzorictinae", "gala",
            "Mycogone", "reappreciate",
            "cyanoguanidine", "seeingness",
            "breadwinner", "noreast",
            "furacious", "epauliere",
            "omniscribent", "Passiflorales",
            "uninductive", "inductivity",
            "Orbitolina", "Semecarpus",
            "migrainoid", "steprelationship",
            "phlogisticate", "mesymnion",
            "sloped", "edificator",
            "beneficent", "culm",
            "paleornithology", "unurban",
            "throbless", "amplexifoliate",
            "sesquiquintile", "sapience",
            "astucious", "dithery",
            "boor", "ambitus",
            "scotching", "uloid",
            "uncompromisingness", "hoove",
            "waird", "marshiness",
            "Jerusalem", "mericarp",
            "unevoked", "benzoperoxide",
            "outguess", "pyxie",
            "hymnic", "euphemize",
            "mendacity", "erythremia",
            "rosaniline", "unchatteled",
            "lienteria", "Bushongo",
            "dialoguer", "unrepealably",
            "rivethead", "antideflation",
            "vinegarish", "manganosiderite",
            "doubtingness", "ovopyriform",
            "Cephalodiscus", "Muscicapa",
            "Animalivora", "angina",
            "planispheric", "ipomoein",
            "cuproiodargyrite", "sandbox",
            "scrat", "Munnopsidae",
            "shola", "pentafid",
            "overstudiousness", "times",
            "nonprofession", "appetible",
            "valvulotomy", "goladar",
            "uniarticular", "oxyterpene",
            "unlapsing", "omega",
            "trophonema", "seminonflammable",
            "circumzenithal", "starer",
            "depthwise", "liberatress",
            "unleavened", "unrevolting",
            "groundneedle", "topline",
            "wandoo", "umangite",
            "ordinant", "unachievable",
            "oversand", "snare",
            "avengeful", "unexplicit",
            "mustafina", "sonable",
            "rehabilitative", "eulogization",
            "papery", "technopsychology",
            "impressor", "cresylite",
            "entame", "transudatory",
            "scotale", "pachydermatoid",
            "imaginary", "yeat",
            "slipped", "stewardship",
            "adatom", "cockstone",
            "skyshine", "heavenful",
            "comparability", "exprobratory",
            "dermorhynchous", "parquet",
            "cretaceous", "vesperal",
            "raphis", "undangered",
            "Glecoma", "engrain",
            "counteractively", "Zuludom",
            "orchiocatabasis", "Auriculariales",
            "warriorwise", "extraorganismal",
            "overbuilt", "alveolite",
            "tetchy", "terrificness",
            "widdle", "unpremonished",
            "rebilling", "sequestrum",
            "equiconvex", "heliocentricism",
            "catabaptist", "okonite",
            "propheticism", "helminthagogic",
            "calycular", "giantly",
            "wingable", "golem",
            "unprovided", "commandingness",
            "greave", "haply",
            "doina", "depressingly",
            "subdentate", "impairment",
            "decidable", "neurotrophic",
            "unpredict", "bicorporeal",
            "pendulant", "flatman",
            "intrabred", "toplike",
            "Prosobranchiata", "farrantly",
            "toxoplasmosis", "gorilloid",
            "dipsomaniacal", "aquiline",
            "atlantite", "ascitic",
            "perculsive", "prospectiveness",
            "saponaceous", "centrifugalization",
            "dinical", "infravaginal",
            "beadroll", "affaite",
            "Helvidian", "tickleproof",
            "abstractionism", "enhedge",
            "outwealth", "overcontribute",
            "coldfinch", "gymnastic",
            "Pincian", "Munychian",
            "codisjunct", "quad",
            "coracomandibular", "phoenicochroite",
            "amender", "selectivity",
            "putative", "semantician",
            "lophotrichic", "Spatangoidea",
            "saccharogenic", "inferent",
            "Triconodonta", "arrendation",
            "sheepskin", "taurocolla",
            "bunghole", "Machiavel",
            "triakistetrahedral", "dehairer",
            "prezygapophysial", "cylindric",
            "pneumonalgia", "sleigher",
            "emir", "Socraticism",
            "licitness", "massedly",
            "instructiveness", "sturdied",
            "redecrease", "starosta",
            "evictor", "orgiastic",
            "squdge", "meloplasty",
            "Tsonecan", "repealableness",
            "swoony", "myesthesia",
            "molecule", "autobiographist",
            "reciprocation", "refective",
            "unobservantness", "tricae",
            "ungouged", "floatability",
            "Mesua", "fetlocked",
            "chordacentrum", "sedentariness",
            "various", "laubanite",
            "nectopod", "zenick",
            "sequentially", "analgic",
            "biodynamics", "posttraumatic",
            "nummi", "pyroacetic",
            "bot", "redescend",
            "dispermy", "undiffusive",
            "circular", "trillion",
            "Uraniidae", "ploration",
            "discipular", "potentness",
            "sud", "Hu",
            "Eryon", "plugger",
            "subdrainage", "jharal",
            "abscission", "supermarket",
            "countergabion", "glacierist",
            "lithotresis", "minniebush",
            "zanyism", "eucalypteol",
            "sterilely", "unrealize",
            "unpatched", "hypochondriacism",
            "critically", "cheesecutter",
    };
}