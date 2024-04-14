using Amazon.S3;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3.Model;
using System.Text.RegularExpressions;
using System.IO;
using System.Configuration;

namespace ConsoleApp1
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Setting up credentials and  file information
            string accessKey = ConfigurationManager.AppSettings["AccessKey"];
            string secretKey = ConfigurationManager.AppSettings["SecretKey"];
            string bucketName = ConfigurationManager.AppSettings["BucketName"];
            string inputFileName = ConfigurationManager.AppSettings["InputFileName"];
            string resultFileName = ConfigurationManager.AppSettings["ResultFileName"];

            try
            {
                // Retrieve file contents from S3
                string fileContents = await GetFileContentsFromS3(inputFileName, accessKey, secretKey, bucketName);

                // Count the words in the text
                Dictionary<string, int> wordsCount = CountWords(fileContents);

                // Upload word count results to S3
                await UploadResultsToS3(wordsCount, accessKey, secretKey, bucketName, resultFileName);

                Console.WriteLine("Word count results uploaded to S3 bucket.");
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occurred:");
                Console.WriteLine(e.Message);
            }
            Console.ReadLine();
        }
        // Function to count words in the text

        static Dictionary<string, int> CountWords(string text)
        {
            Dictionary<string, int> wordsCount = new Dictionary<string, int>();

            var matches = Regex.Matches(text, @"[\w-]+");
            foreach (Match match in matches)
            {
                string word = match.Value.ToLower();
                if (wordsCount.ContainsKey(word))
                {
                    wordsCount[word]++;
                }
                else
                {
                    wordsCount[word] = 1;
                }
            }

            return wordsCount;
        }

        // Function to retrieve file contents from S3

        static async Task<string> GetFileContentsFromS3(string fileName, string accessKey, string secretKey, string bucketName)
        {
            using (var client = new AmazonS3Client(accessKey, secretKey, RegionEndpoint.USEast1))
            {
                var request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = fileName
                };

                using (var response = await client.GetObjectAsync(request))
                using (var reader = new StreamReader(response.ResponseStream))
                {
                    return await reader.ReadToEndAsync();
                }
            }
        }
        //Function to  upload word count results to S3
        static async Task UploadResultsToS3(Dictionary<string, int> wordsCount, string accessKey, string secretKey, string bucketName, string fileName)
        {
            using (var client = new AmazonS3Client(accessKey, secretKey, RegionEndpoint.USEast1))
            {
                string result = string.Join(Environment.NewLine, wordsCount.Select(pair => $"{pair.Key}: {pair.Value}"));
                PutObjectRequest request = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = fileName,
                    ContentBody = result
                };

                await client.PutObjectAsync(request);
            }
        }
    }
}
