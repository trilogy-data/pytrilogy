# AI Interaction Design

## Overview

Core primitive is a conversation. Conversations are made up of messages. Messages have roles (user, assistant, system) and content. 

A provider is used to get an additional response from the AI model. Providers can be swapped out to use different AI services (e.g., OpenAI, Anthropic, etc.).

To get out a structured output, like a query, we use a dedicated conversation method to append a message with a parser. This method will handle the validation 
loop and responses to the AI. 