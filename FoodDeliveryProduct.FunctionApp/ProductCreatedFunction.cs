using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using FoodDeliveryProduct.FunctionApp.Models;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FoodDeliveryProduct.FunctionApp
{
    public class ProductCreatedFunction
    {
        public ProductCreatedFunction()
        {
        }

        [FunctionName("ListenToProductsCreatedMessages")]
        public async Task Run([ServiceBusTrigger("foodelivery-europe-topic", "foodelivery-products", Connection = "foodelivery-product-connectionString", IsSessionsEnabled = true)] Message [] messages)
        {

            ///Add Messages to Bobl containers
            ///for large  amount of broad data and cost effctive

            string uri = Environment.GetEnvironmentVariable("EndpointToBlob");
            string blobName = Environment.GetEnvironmentVariable("Name");
            string sasToWrite = Environment.GetEnvironmentVariable("ToCreatAndAddBlob");
            BlobContainerClient blobContainerClient = new BlobContainerClient(new Uri(uri), new AzureSasCredential(sasToWrite));

            //BlobClient blobClient = new BlobClient(new Uri(uri), new AzureSasCredential(sasToRead));
            AppendBlobClient appendBlobClient = blobContainerClient.GetAppendBlobClient(blobName);

            //Using Stream and a writer to transform the messages to the right format
            ProductCreatedSBM product= await  MapDataToObject(messages);
            MemoryStream stream = await AddDataToStreamAndSendToBlob(product,appendBlobClient);

            /// Add Messages to Tables
            /// for relational data and cost effective

            /* string[] values = await ImplementMessages(messages);

             // All those indos are retrieved from the config -- add them manually after resources are created on azure

             string tableName = Environment.GetEnvironmentVariable("Name");
             string endpoint = Environment.GetEnvironmentVariable("Endpoint"); ;
             string sasToAdd = Environment.GetEnvironmentVariable("ToAddAndWriteTable");
             string rowKey = Environment.GetEnvironmentVariable("RowKey");
             string partitionKey = Environment.GetEnvironmentVariable("PartitionKey");



             //Process to create table and add entity in it
             await InitTableCreation(endpoint, sasToCreate, tableName);
             await AddEntityToTable(partitionKey, rowKey, endpoint, tableName, sasToAdd, values);*/

        }




        //add messages from bytes to string to an array
        /*  private Task<string[]> ImplementMessages(Message[] messages )
          {
              string[] results = new string[] {};

              foreach (var message in messages)
              {
                  string valueFromByte = Encoding.UTF8.GetString(message.Body);
                  string value=valueFromByte.Split(':').LastOrDefault();
                  results = results.Append(value).ToArray();
              }

              return Task.FromResult(results);
          }

          //create a table linked to a storage account with key credentials
          private Task InitTableCreation(string uri, string signature,string tableName)
          {
              var serviceClient = new TableServiceClient(
                                  new Uri(uri),new AzureSasCredential(signature)
                                  );
              serviceClient.CreateTableIfNotExists(tableName);

              return Task.CompletedTask;
          }


          private Task AddEntityToTable(string partitionKey,string rowKey,string uri,string tableName, string signature, string[] values)
          {
              string endpoint = uri + tableName;
              var tableClient = new TableClient(
                  new Uri(endpoint),
                  new AzureSasCredential(signature));


              var tableEntity = new TableEntity(partitionKey, rowKey)
                                  {
                                      { "Id", values[0] },
                                      { "Name", values[1] },
                                      { "Description", values[2] },
                                      { "Price", values[3] },
                                      { "Quantity", values[4] },
                                      { "StoreId", values[5] },

                                  };
              tableClient.AddEntity(tableEntity);

              return Task.CompletedTask;
          }

          */






        ///For blobs 
        ///

        private async Task<MemoryStream> AddDataToStreamAndSendToBlob(ProductCreatedSBM data, AppendBlobClient appendBlobClient)
        {
            //turn to json first
            var jsonData=JsonSerializer.Serialize(data);

            await using MemoryStream stream = new MemoryStream();
            await using StreamWriter writer = new StreamWriter(stream);

            await writer.WriteLineAsync(jsonData);
            await writer.FlushAsync();
            stream.Position = 0;


            await appendBlobClient.CreateIfNotExistsAsync();

            //Configure the Block that will be appended to the blob
            int maxBlockSize = appendBlobClient.AppendBlobMaxAppendBlockBytes;
            long bytesLeft = stream.Length;
            byte[] buffer = new byte[maxBlockSize];
            while (bytesLeft > 0)
            {
                int blockSize = (int)Math.Min(bytesLeft, maxBlockSize);
                int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, blockSize));
                await using (MemoryStream memoryStream = new MemoryStream(buffer, 0, bytesRead))
                {
                    await appendBlobClient.AppendBlockAsync(memoryStream);
                }
                bytesLeft -= bytesRead;
            }
            return await Task.FromResult(stream);
        }



        private Task<ProductCreatedSBM> MapDataToObject(Message[] messages)
        {
            string[] values = new string[] { };
            foreach(var message in messages)
            {
                values = values.Append(Encoding.UTF8.GetString(message.Body)).ToArray();
            }

            ProductCreatedSBM productCreatedSBM = new ProductCreatedSBM()
            {
                Id = values[0],
                Name = values[1],
                Description = values[2],
                Price = values[3],
                Quantity = values[4],
                StoreId = values[5]
            };

            return Task.FromResult(productCreatedSBM);
        }
    }
}
