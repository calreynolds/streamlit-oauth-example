window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        data_streaming_OpenAI_flask_API: async function dataStreamingOpenAIFlaskAPI(n_clicks, question) {
            const chatlog = document.querySelector("#rcw-response");
            
            // Send the messages to the server to stream the response
            const response = await fetch("/dbx-stream", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ question }),
            });
          
            // Create a new TextDecoder to decode the streamed response text
            const decoder = new TextDecoder();
          
            // Set up a new ReadableStream to read the response body
            const reader = response.body.getReader();
            let chunks = "";
            
            // Read the response stream as chunks and append them to the chat log
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks += decoder.decode(value);
                const htmlText = marked.parse(chunks);
                chatlog.innerHTML = htmlText;
            }
            const htmlText = chunks;
            console.log(htmlText);
            return [ false, htmlText, "show"];
          }
    }
});