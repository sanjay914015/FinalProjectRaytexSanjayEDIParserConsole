using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;


namespace Parser
{ 
    internal class Program
    {
        static void Main(string[] args)
        {
            String path = @"C:\Final Project\FinalProjectRaytexSanjayEDIParserConsole\Parser\Parser\EDI.txt";
            System.IO.StreamReader reader;
            reader = new System.IO.StreamReader(path);
            string Data = reader.ReadToEnd();
            reader.Close();
            string[] values = Data.Split('~');

            //Main Json Object

            MainJson mainJson = new MainJson();

            //Container Class Object to store Multiple Value
            SubJson json = new SubJson();

            for (int i = 0; i < values.Length; i++)
            {
                string temp = values[i];
                temp = Regex.Replace(temp, @"\s", "");
                string[] Seg_Id = temp.Split('*');

                if (Convert.ToString(Seg_Id[0]) == "ISA" )
                {
                    string[] ISA = Seg_Id;
                    ISA isa = new ISA()
                    {
                        SenderID = ISA[6],
                        RecieverID = ISA[8],
                        Date = ISA[9],
                        Time = ISA[10],
                        ICSID = ISA[11],
                        InterchangeVersion = ISA[12],
                        ICN = ISA[13],
                        Acknow_request = ISA[14]
                    };
                    mainJson.isa = isa;
                }
                else if (Convert.ToString(Seg_Id[0]) == "GS")
                {
                    string[] GS = Seg_Id;
                    GS gs = new GS()
                    {
                        Functional_Identifier_Code = GS[1],
                        Sender_code = GS[2],
                        Reciever_Code = GS[3],
                        Date = GS[4],
                        Time = GS[5],
                        GCN = GS[6],
                        Agency_Code = GS[7],
                        Version = GS[8],
                    };
                    mainJson.gs = gs;
                }
                else if (Convert.ToString(Seg_Id[0]) == "ST")
                {
                    json.n9.Clear();
                    json.sg.Clear();
                    json.r4.Clear();
                    string[] ST = Seg_Id;
                    ST st = new ST()
                    {
                        Transaction_ID = ST[1],
                        Transaction_CN = ST[2],
                    };
                    json.st = st;
                }
                else if (Convert.ToString(Seg_Id[0]) == "B4")
                {
                    string[] B4 = Seg_Id;
                    B4 b4 = new B4()
                    {
                        Special_Handling_Code = B4[1],
                        Inquery_Request_No = B4[2],
                        Shipement_Status_Code = B4[3],
                        ReleaseDate = B4[4],
                        ReleaseTime = B4[5],
                        Status_Location = B4[6],
                        Equipment_Initial = B4[7],
                        Equipment_Number = B4[8],
                        Equipment_Status_Code = B4[9],
                        Equipment_Type = B4[10]
                    };
                    mainJson.id = B4[7] + B4[8];
                    json.b4 = b4;  
                }
                else if (Convert.ToString(Seg_Id[0]) == "N9")
                {
                    string[] N9 = Seg_Id;
                    N9 n9 = new N9();
                    if (N9[1] == "SCA" || N9[1] == "AAO" || N9[1] == "TI" || N9[1] == "GCD" || N9[1] == "SN" || N9[1] == "ZCD" || N9[1] == "BN" || N9[1] == "BM" || N9[1] == "YS" || N9[1] == "EQ" || N9[1] == "L1" || N9[1] == "TT" || N9[1] == "GC")
                    {
                        n9.Ref_Id_Qualifier = Convert.ToString(N9[1]);
                        n9.Ref_Id = Convert.ToString(N9[2]);
                    }
                    else
                    {
                        n9.Ref_Id_Qualifier = N9[1];
                        n9.Fees = N9[2];
                    }
                    json.n9.Add(n9);
                }
                else if (Convert.ToString(Seg_Id[0]) == "Q2")
                {
                    string[] Q2 = Seg_Id;
                    Q2 q2 = new Q2()
                    {
                        Vessel_Code = Q2[1],
                        Country_Code = Q2[2],
                        Weight = Q2[7],
                        Weight_Qualifier = Q2[8],
                        voyage_Number = Q2[9],
                        Vessel_Name = Q2[13]
                    };
                    json.q2 = q2;
                }
                else if (Convert.ToString(Seg_Id[0]) == "SG")
                {
                    string[] SG = Seg_Id;
                    SG sg = new SG()
                    {
                        Shipment_Status_Code = SG[1],
                        Date = SG[4],
                        Time = SG[5]
                    };
                    json.sg.Add(sg);
                }
                else if (Convert.ToString(Seg_Id[0]) == "R4")
                {
                    string[] R4 = Seg_Id;
                    R4 r4 = new R4()
                    {
                        Port_Function_Code = R4[1],
                        Location_Qualifier = R4[2],
                        Location_Identifier = R4[3],
                        Port_Name = R4[4]
                    };
                    json.r4.Add(r4);
                }
                else if (Convert.ToString(Seg_Id[0]) == "DTM")
                {
                    string[] DTM = Seg_Id;
                    DTM dtm = new DTM()
                    {
                        DTQ = DTM[1],
                        Date = DTM[2],
                        Time = DTM[3]
                    };
                    json.dtm = dtm;
                }
                else if (Convert.ToString(Seg_Id[0]) == "SE")
                {
                    string[] SE = Seg_Id;
                    SE se = new SE()
                    {
                        Number_of_Segments = SE[1],
                        Transaction_Set_Control_Number = SE[2],
                    };
                    json.se = se;

                    mainJson.subcontainer.Add(json);

                    CosmosConnect cosmosConnect = new CosmosConnect();
                    cosmosConnect.CreateDatabaseAsync().Wait();
                    cosmosConnect.CreateContainerAsync().Wait();
                    cosmosConnect.AddItemsToContainerAsync(mainJson).Wait();

                    mainJson.subcontainer.Clear();
                }
                else if (Convert.ToString(Seg_Id[0]) == "GE")
                {
                    string[] GE = Seg_Id;
                    GE ge = new GE()
                    {
                        Number_Transaction_Set = GE[1],
                        GCN = GE[2]
                    };
                    mainJson.ge = ge;
                }
                else if (Convert.ToString(Seg_Id[0]) == "IEA")
                {
                    string[] IEA = Seg_Id;
                    IEA iea = new IEA()
                    {
                        Number_Functional_Group = IEA[1],
                        ICN = IEA[2]
                    };
                    mainJson.iea = iea;
                }
                else
                {
                    Console.WriteLine("EDI File Ended .... ");
                }
            }

        }
        public class MainJson
        {
            [JsonProperty("id")]
            public string id { get; set; }

            [JsonProperty("isa")]
            public ISA isa { get; set; }

            [JsonProperty("gs")]
            public GS gs { get; set; }

            [JsonProperty("subcontainer")]
            public List<SubJson> subcontainer = new List<SubJson>();    

            [JsonProperty("ge")]
            public GE ge { get; set; }

            [JsonProperty("iea")]
            public IEA iea { get; set; }

        }
        public class SubJson
        {
            [JsonProperty("st")]
            public ST st { get; set; }

            [JsonProperty("b4")]
            public B4 b4 { get; set; }

            [JsonProperty("n9")]
            public List<N9> n9 = new List<N9>();

            [JsonProperty("q2")]
            public Q2 q2 { get; set; }

            [JsonProperty("sg")]
            public List<SG> sg = new List<SG>();

            [JsonProperty("r4")]
            public List<R4> r4 = new List<R4>();
            [JsonProperty("dtm")]
            public DTM dtm { get; set; }

            [JsonProperty("se")]
            public SE se { get; set; }
        }
        public class ISA
        {
            [JsonProperty("SenderID")]
            public string SenderID { get; set; }
            [JsonProperty("RecieverID")]
            public string RecieverID { get; set; }
            [JsonProperty("Date")]
            public string Date { get; set; }

            [JsonProperty("Time")]
            public string Time { get; set; }

            [JsonProperty("ICSID")]
            public string ICSID { get; set; }
            [JsonProperty("InterchangeVersion")]
            public string InterchangeVersion { get; set; }
            [JsonProperty("ICN")]
            public string ICN { get; set; }
            [JsonProperty("Acknow_request")]
            public string Acknow_request { get; set; }
            [JsonProperty("Test_Indicator")]
            public string Test_Indicator { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class GS
        {
            [JsonProperty("Functional_Identifier_Code")]
            public string Functional_Identifier_Code { get; set; }
            [JsonProperty("Sender_code")]
            public string Sender_code { get; set; }
            [JsonProperty("Reciever_Code")]
            public string Reciever_Code { get; set; }
            [JsonProperty("GroupDate")]
            public string Date { get; set; }
            [JsonProperty("Date")]
            public string Time { get; set; }
            [JsonProperty("Time")]
            public string GCN { get; set; }
            [JsonProperty("Agency_Code")]
            public string Agency_Code { get; set; }

            [JsonProperty("Version")]
            public string Version { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class ST
        {
            [JsonProperty("Transaction_ID")]
            public string Transaction_ID { get; set; }
            [JsonProperty("Transaction_CN")]
            public string Transaction_CN { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class B4
        {
            [JsonProperty("ContainerID")]
            public string ContainerID { get; set; }
            [JsonProperty("Special_Handling_Code")]
            public string Special_Handling_Code { get; set; }
            [JsonProperty("Inquery_Request_No")]
            public string Inquery_Request_No { get; set; }
            [JsonProperty("Shipement_Status_Code")]
            public string Shipement_Status_Code { get; set; }
            [JsonProperty("ReleaseDate")]
            public string ReleaseDate { get; set; }
            [JsonProperty("ReleaseTime")]
            public string ReleaseTime { get; set; }

            [JsonProperty("Status_Location")]
            public string Status_Location { get; set; }
            [JsonProperty("Equipment_Initial")]
            public string Equipment_Initial { get; set; }
            [JsonProperty("Equipment_Number")]
            public string Equipment_Number { get; set; }
            [JsonProperty("Equipment_Status_Code")]
            public string Equipment_Status_Code { get; set; }

            [JsonProperty("Equipment_Type")]
            public string Equipment_Type { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }

        public class N9
        {
            [JsonProperty("Ref_Id_Qualifier")]

            public string Ref_Id_Qualifier { get; set; }
            [JsonProperty("Ref_Id")]
            public string Ref_Id { get; set; }

            [JsonProperty("Fees")]
            public string Fees { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class Q2
        {
            [JsonProperty("Vessel_Code")]
            public string Vessel_Code { get; set; }
            [JsonProperty("Country_Code")]
            public string Country_Code { get; set; }
            [JsonProperty("voyage_Number")]
            public string voyage_Number { get; set; }
            [JsonProperty("Vessel_Name")]
            public string Vessel_Name { get; set; }
            [JsonProperty("Weight")]
            public string Weight { get; set; }
            [JsonProperty("Weight_Qualifier")]
            public string Weight_Qualifier { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class SG
        {
            [JsonProperty("Shipment_Status_Code")]
            public string Shipment_Status_Code { get; set; }
            [JsonProperty("Date")]
            public string Date { get; set; }

            [JsonProperty("Time")]
            public string Time { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class R4
        {
            [JsonProperty("Port_Function_Code")]
            public string Port_Function_Code { get; set; }
            [JsonProperty("Location_Qualifier")]
            public string Location_Qualifier { get; set; }
            [JsonProperty("Location_Identifier")]
            public string Location_Identifier { get; set; }
            [JsonProperty("Port_Name")]
            public string Port_Name { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class DTM
        {
            [JsonProperty("DTQ")]
            public string DTQ { get; set; }

            [JsonProperty("Date")]
            public string Date { get; set; }

            [JsonProperty("Time")]
            public string Time { get; set; }

            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class SE
        {
            [JsonProperty("Number_of_Segments")]
            public string Number_of_Segments { get; set; }

            [JsonProperty("Transaction_Set_Control_Number")]
            public string Transaction_Set_Control_Number { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class GE
        {
            [JsonProperty("Number_Transaction_Set")]
            public string Number_Transaction_Set { get; set; }

            [JsonProperty("GCN")]
            public string GCN { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class IEA
        {
            [JsonProperty("Number_Functional_Group")]
            public string Number_Functional_Group { get; set; }

            [JsonProperty("ICN")]
            public string ICN { get; set; }
            public override string ToString()
            {
                return JsonConvert.SerializeObject(this);
            }
        }
        public class CosmosConnect
        {
            private static readonly string EndpointURI = "https://localhost:8081";
            private static readonly string PrimaryKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

            private CosmosClient cosmosClient;
            private Database database;
            private Container container;

            private string databaseId = "ediDB";
            private string containerId = "EdiContainer";

            public CosmosConnect()
            {
                try
                {
                    Console.WriteLine("Establish Connection...");
                    this.cosmosClient = new CosmosClient(EndpointURI,
                    PrimaryKey);

                }
                catch (CosmosException cosmosException)
                {
                    Console.WriteLine($"CosmosDb exception {cosmosException.StatusCode}: {cosmosException} ");
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error: {e}");
                }

            }
            public async Task CreateDatabaseAsync()
            {
                // Create a new database
                this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
                Console.WriteLine($"Created Database: {this.database.Id}");
            }
            public async Task CreateContainerAsync()
            {
                // Create a new container
                this.container = await this.database.CreateContainerIfNotExistsAsync(this.containerId, "/id");
                Console.WriteLine($"Created Container: {this.containerId}");
            }
            public async Task AddItemsToContainerAsync(MainJson mainjson)
            {
                try
                {
                    //Read the item to see if it exists
                    ItemResponse<MainJson> jsondata =
                    await this.container.
                    ReadItemAsync<MainJson>(mainjson.id,
                    new PartitionKey(mainjson.id));
                    Console.WriteLine("Item in database with" +
                    " id: {0} already exists\n",
                    jsondata.Resource.id);
                }
                catch (CosmosException ex) when
                (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // Create an item in the container representing the Andersen family. Note we provide the value of the partition key for this item, which is "Andersen"
                    ItemResponse<MainJson> jsondata =
                    await this.container.
                    CreateItemAsync<MainJson>(mainjson,
                    new PartitionKey(mainjson.id));
                    // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                    Console.WriteLine(
                    $"Created item in database with id: {jsondata.Resource.id} Operation consumed {jsondata.RequestCharge} RUs.\n");
                }
            }
        }
    }
}

