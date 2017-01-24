package com.trevjonez.kspark2

import com.amazonaws.serverless.exceptions.ContainerInitializationException
import com.amazonaws.serverless.proxy.internal.model.AwsProxyRequest
import com.amazonaws.serverless.proxy.internal.model.AwsProxyResponse
import com.amazonaws.serverless.proxy.spark.SparkLambdaContainerHandler
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.log4j.LambdaAppender
import org.apache.log4j.Logger
import spark.Spark.get

/**
 * @author TrevJonez
 */
class LambdaHandler : RequestHandler<AwsProxyRequest, AwsProxyResponse> {
    var isInitialized = false
    lateinit var handler: SparkLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse>

    override fun handleRequest(input: AwsProxyRequest?, context: Context?): AwsProxyResponse? {
        if (!isInitialized) {
            isInitialized = true
            try {
                Logger.getRootLogger().addAppender(LambdaAppender())
                handler = SparkLambdaContainerHandler.getAwsProxyHandler()
                defineResources()
            } catch (error: ContainerInitializationException) {
                error.printStackTrace()
                return null
            }
        }
        return handler.proxy(input, context)
    }

    private fun defineResources() {
        get("/hi", { request, response ->  "hello world"})
    }
}