package index

type LanguageSetting struct {
	Name     string
	Settings string
}

var DefaultLanguageSetting = LanguageSetting{
	Name:     "standard",
	Settings: "",
}

// These are the language-specific settings that Opensearch can handle.
var LanguageSettings = map[string]LanguageSetting{
	"ar": {
		Name: "arabic",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "arabic"
						}
					}
				}
			}
		}`,
	},
	"hy": {
		Name: "armenian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "armenian"
						}
					}
				}
			}
		}`,
	},
	"eu": {
		Name: "basque",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "basque"
						}
					}
				}
			}
		}`,
	},
	"bn": {
		Name: "bengali",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "bengali"
						}
					}
				}
			}
		}`,
	},
	"bg": {
		Name: "bulgarian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "bulgarian"
						}
					}
				}		
			}
		}`,
	},
	"ca": {
		Name: "catalan",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "catalan"
						}
					}
				}
			}
		}`,
	},
	"cs": {
		Name: "czech",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "czech"
						}
					}
				}
			}
		}`,
	},
	"da": {
		Name: "danish",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "danish"
						}
					}
				}
			}
		}`,
	},
	"nl": {
		Name: "dutch",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "dutch"
						}
					}
				}
			}
		}`,
	},
	"en": {
		Name: "english",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "english"
						}
					}
				}
			}
		}`,
	},
	"et": {
		Name: "estonian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "estonian"
						}
					}
				}
			}
		}`,
	},
	"fi": {
		Name: "finnish",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "finnish"
						}
					}
				}
			}
		}`,
	},
	"fr": {
		Name: "french",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "french"
						}
					}
				}
			}
		}`,
	},
	"gl": {
		Name: "galician",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "galician"
						}
					}
				}
			}
		}`,
	},
	"de": {
		Name: "german",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "german"
						}
					}
				}
			}
		}`,
	},
	"el": {
		Name: "greek",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "greek"
						}
					}
				}
			}
		}`,
	},
	"hi": {
		Name: "hindi",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "hindi"
						}
					}
				}
			}
		}`,
	},
	"hu": {
		Name: "hungarian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "hungarian"
						}
					}
				}
			}
		}`,
	},
	"ic": {
		Name: "indonesian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "indonesian"
						}
					}
				}
			}
		}`,
	},
	"ga": {
		Name: "irish",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "irish"
						}
					}
				}
			}
		}`,
	},
	"it": {
		Name: "italian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "italian"
						}
					}
				}
			}
		}`,
	},
	"lv": {
		Name: "latvian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "latvian"
						}
					}
				}
			}
		}`,
	},
	"lt": {
		Name: "lithuanian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "lithuanian"
						}
					}
				}
			}
		}`,
	},
	"no": {
		Name: "norwegian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "norwegian"
						}
					}
				}
			}
		}`,
	},
	"fa": {
		Name: "persian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "persian"
						}
					}
				}
			}
		}`,
	},
	"pt": {
		Name: "portuguese",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "portuguese"
						}
					}
				}
			}
		}`,
	},
	"ro": {
		Name: "romanian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "romanian"
						}
					}
				}
			}
		}`,
	},
	"ru": {
		Name: "russian",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "russian"
						}
					}
				}
			}
		}`,
	},
	"es": {
		Name: "spanish",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "spanish"
						}
					}
				}
			}
		}`,
	},
	"sv": {
		Name: "swedish",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "swedish"
						}
					}
				}
			}
		}`,
	},
	"tr": {
		Name: "turkish",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "turkish"
						}
					}
				}
			}
		}`,
	},
	"th": {
		Name: "thai",
		Settings: `{
			"settings": {
				"analysis": {
					"analyzer": {
						"default": {
							"type": "thai"
						}
					}
				}
			}
		}`,
	},
}
