const fileInput = document.getElementById('files');
        const dropArea = document.getElementById('drop-area');
        const errorMessage = document.getElementById('error-message');

        dropArea.addEventListener('click', () => {
            fileInput.click();
        });

        dropArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropArea.classList.add('dragover');
        });

        dropArea.addEventListener('dragleave', () => {
            dropArea.classList.remove('dragover');
        });

        dropArea.addEventListener('drop', (e) => {
            e.preventDefault();
            dropArea.classList.remove('dragover');

            const files = e.dataTransfer.files;
            if (validateFiles(files)) {
                fileInput.files = files;
                updateFileList(files);
            } else {
                showError("Only Excel or CSV files are allowed.");
            }
        });

        fileInput.addEventListener('change', () => {
            if (validateFiles(fileInput.files)) {
                updateFileList(fileInput.files);
                clearError();
            } else {
                showError("Only Excel or CSV files are allowed.");
                fileInput.value = "";  // Reset the input
            }
        });

        function validateFiles(files) {
            const validExtensions = ['xls', 'xlsx', 'csv'];
            for (let file of files) {
                const fileExtension = file.name.split('.').pop().toLowerCase();
                if (!validExtensions.includes(fileExtension)) {
                    return false;
                }
            }
            return true;
        }

        function updateFileList(files) {
            const fileNames = Array.from(files).map(file => file.name).join(', ');
            const message = document.querySelector('.file-drop-message');
            message.textContent = `Selected files: ${fileNames}`;
        }

        function showError(message) {
            errorMessage.textContent = message;
        }

        function clearError() {
            errorMessage.textContent = '';
        }