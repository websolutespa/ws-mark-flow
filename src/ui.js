function converterApp() {
        return {
            activeTab: 'sources',
            loading: false,
            alert: { show: false, message: '', type: 'success' },
            
            // Data
            sources: [],
            destinations: [],
            jobs: [],
            schemas: [],

            // New Job modal
            showNewJobModal: false,

            // Config modal
            showConfigModal: false,
            editingConfig: null,
            configModal: {
                isSource: true,
                name: '',
                description: '',
                type: '',
                config: {}
            },
            currentSchemaFields: [],
            
            // Test modal
            showTestModal: false,
            testResult: { success: false, message: '', sample_files: [] },
            
            // Analysis modal
            showAnalysisModal: false,
            analysisResult: {},
            currentAnalysisJobId: null,
            
            // Edit job modal
            showEditJobModal: false,
            editingJob: null,
            editJobForm: {
                name: '',
                description: '',
                extensionsStr: '',
                source_folder: '',
                destination_folder: '',
                conversion_strategy: 'fast',
                batch_size: 4,
                // LLM overrides
                llm_provider: '',
                llm_model: '',
                llm_api_key: '',
                llm_api_key_set: false,
                llm_base_url: '',
                llm_max_pages: null,
                pdf_complexity_threshold: null,
                // Schedule
                schedule_cron: '',
                schedule_enabled: false
            },

            // History modal
            showHistoryModal: false,
            historyRecords: [],
            historyJobName: '',

            // New job form
            newJob: {
                name: '',
                description: '',
                source_config_id: '',
                destination_config_id: '',
                extensionsStr: '.pdf, .docx, .pptx, .xlsx, .csv',
                source_folder: '',
                destination_folder: '',
                conversion_strategy: 'fast',
                batch_size: 4,
                // LLM overrides
                llm_provider: '',
                llm_model: '',
                llm_api_key: '',
                llm_base_url: '',
                llm_max_pages: null,
                pdf_complexity_threshold: null,
                // Schedule
                schedule_cron: '',
                schedule_enabled: false
            },
            
            get canCreateJob() {
                return this.newJob.name && this.newJob.source_config_id && this.newJob.destination_config_id;
            },
            
            async init() {
                await this.loadSchemas();
                await this.loadSources();
                await this.loadDestinations();
                await this.loadJobs();
            },
            
            showAlert(message, type = 'success') {
                this.alert = { show: true, message, type };
                setTimeout(() => this.alert.show = false, 5000);
            },
            
            async api(url, options = {}) {
                this.loading = true;
                try {
                    const response = await fetch(url, {
                        headers: { 'Content-Type': 'application/json', ...options.headers },
                        ...options
                    });
                    const data = await response.json();
                    if (!response.ok) throw new Error(data.detail || 'Request failed');
                    return data;
                } finally {
                    this.loading = false;
                }
            },
            
            async loadSchemas() {
                try {
                    this.schemas = await this.api('/schemas');
                } catch (e) {
                    console.error('Failed to load schemas:', e);
                }
            },
            
            async loadSources() {
                try {
                    this.sources = await this.api('/configurations/sources');
                } catch (e) {
                    console.error('Failed to load sources:', e);
                }
            },
            
            async loadDestinations() {
                try {
                    this.destinations = await this.api('/configurations/destinations');
                } catch (e) {
                    console.error('Failed to load destinations:', e);
                }
            },
            
            async loadJobs() {
                try {
                    this.jobs = await this.api('/jobs');
                } catch (e) {
                    console.error('Failed to load jobs:', e);
                }
            },

            openConfigModal(isSource) {
                this.editingConfig = null;
                this.configModal = {
                    isSource,
                    name: '',
                    description: '',
                    type: '',
                    config: {}
                };
                this.currentSchemaFields = [];
                this.showConfigModal = true;
            },
            
            editConfig(config) {
                this.editingConfig = config;
                this.configModal = {
                    isSource: config.is_source,
                    name: config.name,
                    description: config.description || '',
                    type: config.type,
                    config: { ...config.config }
                };
                this.loadSchemaFields();
                this.showConfigModal = true;
            },
            
            closeConfigModal() {
                this.showConfigModal = false;
                this.editingConfig = null;
            },
            
            loadSchemaFields() {
                const schema = this.schemas.find(s => s.type === this.configModal.type);
                this.currentSchemaFields = schema ? schema.fields : [];
                
                // Initialize empty config values for fields
                for (const field of this.currentSchemaFields) {
                    if (!(field.name in this.configModal.config)) {
                        this.configModal.config[field.name] = '';
                    } else if (field.field_type === 'textarea' && typeof this.configModal.config[field.name] === 'object') {
                        // Serialize objects/arrays to JSON for textarea display
                        this.configModal.config[field.name] = JSON.stringify(this.configModal.config[field.name], null, 2);
                    }
                }
            },
            
            async saveConfig() {
                try {
                    // Clean empty values from config
                    const config = {};
                    for (const [key, value] of Object.entries(this.configModal.config)) {
                        if (value !== '' && value !== null && value !== undefined) {
                            // Try to parse JSON strings from textarea fields
                            const field = this.currentSchemaFields.find(f => f.name === key);
                            if (field && field.field_type === 'textarea' && typeof value === 'string') {
                                const trimmed = value.trim();
                                if ((trimmed.startsWith('[') && trimmed.endsWith(']')) || (trimmed.startsWith('{') && trimmed.endsWith('}'))) {
                                    try {
                                        config[key] = JSON.parse(trimmed);
                                        continue;
                                    } catch (e) { /* keep as string */ }
                                }
                            }
                            config[key] = value;
                        }
                    }
                    
                    if (this.editingConfig) {
                        await this.api(`/configurations/${this.editingConfig._id}`, {
                            method: 'PATCH',
                            body: JSON.stringify({
                                name: this.configModal.name,
                                description: this.configModal.description,
                                config
                            })
                        });
                        this.showAlert('Configuration updated successfully');
                    } else {
                        await this.api('/configurations', {
                            method: 'POST',
                            body: JSON.stringify({
                                name: this.configModal.name,
                                description: this.configModal.description,
                                type: this.configModal.type,
                                config,
                                is_source: this.configModal.isSource
                            })
                        });
                        this.showAlert('Configuration created successfully');
                    }
                    
                    this.closeConfigModal();
                    await this.loadSources();
                    await this.loadDestinations();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            async deleteConfig(configId) {
                if (!confirm('Are you sure you want to delete this configuration?')) return;
                
                try {
                    await this.api(`/configurations/${configId}`, { method: 'DELETE' });
                    this.showAlert('Configuration deleted');
                    await this.loadSources();
                    await this.loadDestinations();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            async testConfig(configId) {
                try {
                    this.testResult = await this.api(`/configurations/${configId}/test`, { method: 'POST' });
                    this.showTestModal = true;
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            // Build llm_settings payload from a form object; returns null when all fields are empty/default
            _buildLLMSettings(form) {
                const s = {};
                if (form.llm_provider)                      s.llm_provider = form.llm_provider;
                if (form.llm_model)                         s.llm_model = form.llm_model;
                if (form.llm_api_key)                       s.llm_api_key = form.llm_api_key;
                if (form.llm_base_url)                      s.llm_base_url = form.llm_base_url;
                if (form.llm_max_pages != null && form.llm_max_pages !== '') s.llm_max_pages = form.llm_max_pages;
                if (form.pdf_complexity_threshold != null && form.pdf_complexity_threshold !== '') s.pdf_complexity_threshold = form.pdf_complexity_threshold;
                return Object.keys(s).length ? s : null;
            },

            async createJob() {
                try {
                    const extensions = this.newJob.extensionsStr
                        .split(',')
                        .map(e => e.trim())
                        .filter(e => e);
                    
                    await this.api('/jobs/from-configs', {
                        method: 'POST',
                        body: JSON.stringify({
                            name: this.newJob.name,
                            description: this.newJob.description,
                            source_config_id: this.newJob.source_config_id,
                            destination_config_id: this.newJob.destination_config_id,
                            source_extensions: extensions,
                            source_folder: this.newJob.source_folder || null,
                            destination_folder: this.newJob.destination_folder || null,
                            conversion_strategy: this.newJob.conversion_strategy,
                            batch_size: this.newJob.batch_size,
                            llm_settings: this.newJob.conversion_strategy === 'accurate' ? this._buildLLMSettings(this.newJob) : null,
                            schedule_cron: this.newJob.schedule_cron || null,
                            schedule_enabled: this.newJob.schedule_enabled
                        })
                    });
                    
                    this.showAlert('Job created successfully');
                    this.resetNewJob();
                    this.showNewJobModal = false;
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            async createAndRunJob() {
                try {
                    const extensions = this.newJob.extensionsStr
                        .split(',')
                        .map(e => e.trim())
                        .filter(e => e);
                    
                    const job = await this.api('/jobs/from-configs', {
                        method: 'POST',
                        body: JSON.stringify({
                            name: this.newJob.name,
                            description: this.newJob.description,
                            source_config_id: this.newJob.source_config_id,
                            destination_config_id: this.newJob.destination_config_id,
                            source_extensions: extensions,
                            source_folder: this.newJob.source_folder || null,
                            destination_folder: this.newJob.destination_folder || null,
                            conversion_strategy: this.newJob.conversion_strategy,
                            batch_size: this.newJob.batch_size,
                            llm_settings: this.newJob.conversion_strategy === 'accurate' ? this._buildLLMSettings(this.newJob) : null,
                            schedule_cron: this.newJob.schedule_cron || null,
                            schedule_enabled: this.newJob.schedule_enabled
                        })
                    });
                    
                    await this.api(`/jobs/${job._id}/run`, { method: 'POST' });
                    
                    this.showAlert('Job created and started');
                    this.resetNewJob();
                    this.showNewJobModal = false;
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            resetNewJob() {
                this.newJob = {
                    name: '',
                    description: '',
                    source_config_id: '',
                    destination_config_id: '',
                    extensionsStr: '.pdf, .docx, .pptx, .xlsx, .csv',
                    source_folder: '',
                    destination_folder: '',
                    conversion_strategy: 'fast',
                    batch_size: 4,
                    llm_provider: '',
                    llm_model: '',
                    llm_api_key: '',
                    llm_base_url: '',
                    llm_max_pages: null,
                    pdf_complexity_threshold: null,
                    schedule_cron: '',
                    schedule_enabled: false
                };
            },

            openEditJobModal(job) {
                this.editingJob = job;
                // Try to find matching source/destination config by type+config
                const matchSource = this.sources.find(s => 
                    s.type === job.source.type && 
                    JSON.stringify(s.config) === JSON.stringify(job.source.config)
                );
                const matchDest = this.destinations.find(d => 
                    d.type === job.destination.type && 
                    JSON.stringify(d.config) === JSON.stringify(job.destination.config)
                );
                const llm = job.llm_settings || {};
                this.editJobForm = {
                    name: job.name,
                    description: job.description || '',
                    source_config_id: matchSource ? matchSource._id : '',
                    destination_config_id: matchDest ? matchDest._id : '',
                    extensionsStr: (job.source_extensions || []).join(', '),
                    source_folder: job.source_folder || '',
                    destination_folder: job.destination_folder || '',
                    conversion_strategy: job.conversion_strategy || 'fast',
                    batch_size: job.batch_size || 4,
                    llm_provider: llm.llm_provider || '',
                    llm_model: llm.llm_model || '',
                    llm_api_key: '',
                    llm_api_key_set: !!llm.llm_api_key,
                    llm_base_url: llm.llm_base_url || '',
                    llm_max_pages: llm.llm_max_pages ?? null,
                    pdf_complexity_threshold: llm.pdf_complexity_threshold ?? null,
                    schedule_cron: job.schedule_cron || '',
                    schedule_enabled: job.schedule_enabled || false
                };
                this.showEditJobModal = true;
            },

            async saveJobEdit() {
                try {
                    const extensions = this.editJobForm.extensionsStr
                        .split(',')
                        .map(e => e.trim())
                        .filter(e => e);

                    const payload = {
                        name: this.editJobForm.name,
                        description: this.editJobForm.description || null,
                        source_extensions: extensions,
                        source_folder: this.editJobForm.source_folder || null,
                        destination_folder: this.editJobForm.destination_folder || null,
                        conversion_strategy: this.editJobForm.conversion_strategy,
                        batch_size: this.editJobForm.batch_size,
                        llm_settings: this.editJobForm.conversion_strategy === 'accurate' ? this._buildLLMSettings(this.editJobForm) : null,
                        schedule_cron: this.editJobForm.schedule_cron || null,
                        schedule_enabled: this.editJobForm.schedule_enabled
                    };

                    // Resolve source config
                    if (this.editJobForm.source_config_id) {
                        const src = this.sources.find(s => s._id === this.editJobForm.source_config_id);
                        if (src) {
                            payload.source = { type: src.type, config: src.config };
                        }
                    }
                    // Resolve destination config
                    if (this.editJobForm.destination_config_id) {
                        const dest = this.destinations.find(d => d._id === this.editJobForm.destination_config_id);
                        if (dest) {
                            payload.destination = { type: dest.type, config: dest.config };
                        }
                    }

                    await this.api(`/jobs/${this.editingJob._id}`, {
                        method: 'PATCH',
                        body: JSON.stringify(payload)
                    });

                    this.showAlert('Job updated successfully');
                    this.showEditJobModal = false;
                    this.editingJob = null;
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            async runJob(jobId) {
                try {
                    await this.api(`/jobs/${jobId}/run`, { method: 'POST' });
                    this.showAlert('Job started');
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            async analyzeJob(jobId) {
                try {
                    this.currentAnalysisJobId = jobId;
                    this.analysisResult = await this.api(`/jobs/${jobId}/analyze`, { method: 'POST' });
                    this.showAnalysisModal = true;
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            
            async runAnalyzedJob() {
                if (!this.currentAnalysisJobId) return;
                
                try {
                    await this.api(`/jobs/${this.currentAnalysisJobId}/run`, { method: 'POST' });
                    this.showAlert('Job started');
                    this.showAnalysisModal = false;
                    this.currentAnalysisJobId = null;
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            async cancelJob(jobId) {
                try {
                    await this.api(`/jobs/${jobId}/cancel`, { method: 'POST' });
                    this.showAlert('Job cancellation requested');
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },
            async deleteJob(jobId) {
                if (!confirm('Are you sure you want to delete this job?')) return;
                
                try {
                    await this.api(`/jobs/${jobId}`, { method: 'DELETE' });
                    this.showAlert('Job deleted');
                    await this.loadJobs();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },

            async exportConfigs(isSource) {
                try {
                    const label = isSource ? 'sources' : 'destinations';
                    const data = await this.api(`/configurations/export?is_source=${isSource}`);
                    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = `${label}.json`;
                    a.click();
                    URL.revokeObjectURL(url);
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },

            async openHistoryModal(jobId, jobName) {
                try {
                    this.historyJobName = jobName;
                    this.historyRecords = await this.api(`/jobs/${jobId}/history`);
                    this.showHistoryModal = true;
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            },

            formatDuration(start, end) {
                const ms = new Date(end) - new Date(start);
                if (ms < 1000) return ms + 'ms';
                const s = Math.floor(ms / 1000);
                if (s < 60) return s + 's';
                const m = Math.floor(s / 60);
                const rs = s % 60;
                if (m < 60) return m + 'm ' + rs + 's';
                const h = Math.floor(m / 60);
                return h + 'h ' + (m % 60) + 'm';
            },

            async importConfigs(event, isSource) {
                const file = event.target.files[0];
                if (!file) return;
                // Reset file input so the same file can be re-imported later
                event.target.value = '';
                try {
                    const text = await file.text();
                    let items;
                    try {
                        items = JSON.parse(text);
                    } catch {
                        this.showAlert('Invalid JSON file', 'error');
                        return;
                    }
                    if (!Array.isArray(items)) {
                        this.showAlert('JSON must be an array of configurations', 'error');
                        return;
                    }
                    // Force is_source to match the current tab
                    items = items.map(i => ({ ...i, is_source: isSource }));
                    const result = await this.api('/configurations/import', {
                        method: 'POST',
                        body: JSON.stringify(items)
                    });
                    const msg = `Imported ${result.created} configuration(s)` +
                        (result.skipped ? `, ${result.skipped} skipped (duplicate name)` : '') +
                        (result.errors.length ? `, ${result.errors.length} error(s)` : '') + '.';
                    this.showAlert(msg, result.errors.length ? 'error' : 'success');
                    await this.loadSources();
                    await this.loadDestinations();
                } catch (e) {
                    this.showAlert(e.message, 'error');
                }
            }
        };
    }