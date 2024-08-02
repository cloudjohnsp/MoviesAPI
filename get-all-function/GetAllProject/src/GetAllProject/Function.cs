using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GetAllProject;

public class Function
{
    private static readonly string TableName = "movies";
    private static readonly RegionEndpoint Region = RegionEndpoint.USEast1;
    private readonly AmazonDynamoDBClient _dynamoDbClient;

    public Function()
    {
        _dynamoDbClient = new AmazonDynamoDBClient(Region);
    }

    public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest request, ILambdaContext context)
    {
        context.Logger.LogLine("Received request to scan DynamoDB table.");

        var items = await ScanTableAsync();
        var formattedItems = new List<Dictionary<string, object>>();

        foreach (var item in items)
        {
            formattedItems.Add(DocumentToDictionary(item));
        }

        var json = JsonConvert.SerializeObject(formattedItems, Formatting.Indented);

        return new APIGatewayProxyResponse
        {
            StatusCode = 200,
            Body = json,
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }

    private async Task<List<Document>> ScanTableAsync()
    {
        var table = Table.LoadTable(_dynamoDbClient, TableName);
        var scanFilter = new ScanFilter();
        var search = table.Scan(scanFilter);

        var allItems = new List<Document>();
        do
        {
            var documents = await search.GetNextSetAsync();
            allItems.AddRange(documents);
        } while (!search.IsDone);

        return allItems;
    }

    private Dictionary<string, object> DocumentToDictionary(Document doc)
    {
        var dictionary = new Dictionary<string, object>();

        foreach (var attribute in doc.GetAttributeNames())
        {
            var value = doc[attribute];
            dictionary[attribute] = ConvertDynamoDBValue(value);
        }

        return dictionary;
    }

    private object ConvertDynamoDBValue(DynamoDBEntry entry)
    {
        if (entry is Primitive primitive)
        {
            return primitive.Value;
        }
        else if (entry is Document document)
        {
            var dict = new Dictionary<string, object>();
            foreach (var attribute in document.GetAttributeNames())
            {
                dict[attribute] = ConvertDynamoDBValue(document[attribute]);
            }
            return dict;
        }
        else if (entry is DynamoDBList list)
        {
            var listValues = new List<object>();
            foreach (var item in list.Entries)
            {
                listValues.Add(ConvertDynamoDBValue(item));
            }
            return listValues;
        }
        else if (entry is PrimitiveList primitiveList)
        {
            var listValues = new List<object>();
            foreach (var item in primitiveList.Entries)
            {
                listValues.Add(ConvertDynamoDBValue(item));
            }
            return listValues;
        }
        return entry.ToString()!;
    }
}
