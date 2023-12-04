<template>
    <h3>{{ title }}</h3>
    <SQL  :query=query></SQL>
    <button class="submit-button" v-if="data.length == 0" @click="getData" type='submit'>Run</button>
    <button v-else @click="show_results = !show_results">Toggle results</button>
    <div v-if="error">{{ error }}</div>
    <ResultComponent v-if="show_results" :id="title"  :data="data" :fields="headers" :gen_sql="gen_sql" />
</template>
    
<style>
/* CSS */
.submit-button {
    background-color: #c2fbd7;
    border-radius: 100px;
    box-shadow: rgba(44, 187, 99, .2) 0 -25px 18px -14px inset, rgba(44, 187, 99, .15) 0 1px 2px, rgba(44, 187, 99, .15) 0 2px 4px, rgba(44, 187, 99, .15) 0 4px 8px, rgba(44, 187, 99, .15) 0 8px 16px, rgba(44, 187, 99, .15) 0 16px 32px;
    color: green;
    cursor: pointer;
    display: inline-block;
    font-family: CerebriSans-Regular, -apple-system, system-ui, Roboto, sans-serif;
    padding: 5px 20px;
    text-align: center;
    text-decoration: none;
    transition: all 250ms;
    border: 0;
    font-size: 12px;
    user-select: none;
    -webkit-user-select: none;
    touch-action: manipulation;
}

.submit-button:hover {
    box-shadow: rgba(44, 187, 99, .35) 0 -25px 18px -14px inset, rgba(44, 187, 99, .25) 0 1px 2px, rgba(44, 187, 99, .25) 0 2px 4px, rgba(44, 187, 99, .25) 0 4px 8px, rgba(44, 187, 99, .25) 0 8px 16px, rgba(44, 187, 99, .25) 0 16px 32px;
    transform: scale(1.05) rotate(-1deg);
}
</style>
<script>
import axios from 'axios';
// import TableComponent from './TableComponent.vue';
import ResultComponent from './ResultComponent.vue';
import SQL from './SQL.vue'

export default {
    data() {
        return {
            data: [],
            headers: [],
            show_results: false,
            error: null,
            gen_sql: '',

        };
    },
    components: {
        ResultComponent,
        SQL,
        // TableComponent,
    },
    props: {
        query: String,
        title: String,
        dependencies: {
            type: Array,
            default: () => [],
        }

    },

    methods: {
        async getData() {
            this.error = null;
            let queries = this.dependencies.concat([this.query]);
            const joinedString = queries.join(' ');
            try {
                const response = await axios.post(
                    "https://preql-demo-backend-cz7vx4oxuq-uc.a.run.app/query",
                    { 'model': 'titanic', 'query': joinedString }
                );
                this.headers = response.data.headers;
                this.data = response.data.results;
                this.gen_sql = response.data.generated_sql;
                this.show_results = true;
            } catch (error) {
                this.error = error;
                console.log(error);
            }
        },
    },
};
</script>
<style></style>
