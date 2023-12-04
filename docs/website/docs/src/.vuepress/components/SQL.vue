<template>
    <div class="language-sql line-numbers-mode">
        <pre class="language-sql" data-ext="sql"><code ref="sqlRef"
        class="hljs language-sql" >{{ query }}</code></pre>
    </div>
</template>

<script>
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';
import 'highlight.js/styles/github.css';
hljs.registerLanguage('sql', sql);

import { ref, onMounted } from 'vue'

const sqlRef = ref(null)

export default {
    name: 'SQL',
    // Your component options here
    props: {
        query: String,
    },
    mounted() {
        this.highlightSQL()
    },

    methods: {
        highlightSQL() {
            console.log(this.$refs.sqlRef)
            if (this.$refs.sqlRef) {
                hljs.highlightElement(this.$refs.sqlRef)
            }
        }
    },
}
</script>

<style scoped>
/* Your component styles here */
</style>
