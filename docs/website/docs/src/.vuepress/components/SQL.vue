<template>
    <div :style="inputStyles" class="pre-wrapper language-sql">
        <button class="copy-button" @click="copyToClipboard">
            Copy
        </button>
        <pre class="language-sql" data-ext="sql"><code  ref="sqlRef"
        class="hljs language-sql" >{{ query }}</code></pre>
    </div>
</template>

<style scoped>
.copy-button {
    /* minimal, right aligned */
    border-radius: 100px;
    opacity: 1;
    color: gray;
    cursor: pointer;
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
    float: right;
    margin: 10px;

}

.pre-wrapper {
    overflow: auto;
}
</style>

<script>
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';
import 'highlight.js/styles/github.css';
import { useWindowSize } from 'vue-window-size';

hljs.registerLanguage('sql', sql);

import { ref, onMounted } from 'vue'

const sqlRef = ref(null)

export default {
    name: 'SQL',
    // Your component options here
    data() {
        const { width, height } = useWindowSize();
        return {
            windowWidth: width,
            windowHeight: height,
        };

    },
    props: {
        query: String,
        maxWidthScaling: {
            type: Number,
            default: 1.0,
        },
    },
    mounted() {
        this.highlightSQL()
    },
    computed: {
        maxWidth() {
            return this.windowWidth < 600 ? this.windowWidth * 1 * this.maxWidthScaling : this.windowWidth * .6 * this.maxWidthScaling
        },
        fontsize() {
            return this.windowWidth < 600 ? .7 : 1
        },
        inputStyles() {
            return {
                'max-width': this.maxWidth + 'px',
                'font-size': this.fontsize + 'rem',
                'text-align': 'center',
            }
        }
    },

    methods: {
        highlightSQL() {
            console.log(this.$refs.sqlRef)
            if (this.$refs.sqlRef) {
                hljs.highlightElement(this.$refs.sqlRef)
            }
        },
        copyToClipboard() {
            const textArea = document.createElement('textarea');
            textArea.value = this.query;

            // Set the textarea to be invisible
            textArea.style.position = 'fixed';
            textArea.style.top = '0';
            textArea.style.left = '0';
            textArea.style.width = '2em';
            textArea.style.height = '2em';
            textArea.style.padding = '0';
            textArea.style.border = 'none';
            textArea.style.outline = 'none';
            textArea.style.boxShadow = 'none';
            textArea.style.background = 'transparent';

            document.body.appendChild(textArea);
            textArea.select();

            try {
                const successful = document.execCommand('copy');
                const message = successful ? 'Text copied!' : 'Unable to copy text.';
                console.log(message);
            } catch (err) {
                console.error('Failed to copy:', err);
            }

            document.body.removeChild(textArea);
        }
    },
}
</script>
