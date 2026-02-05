---
name: meeting-summarizer
description: Extract key points from meeting documents and generate structured summaries. Use when processing meeting notes, transcripts, or minutes (text/Markdown format) for: team meetings, project updates, client meetings, cross-functional coordination, or remote meeting documentation.
---

# Meeting Summarizer

Extract key information from meetings and generate structured, concise summaries.

## Summary Structure

```
# Meeting Summary - [Topic/Date]

## Participants
- [List key participants]

## Key Decisions
- [Decision 1]
- [Decision 2]

## Action Items
- **[Owner]**: [Task] (Deadline: [Date])
- **[Owner]**: [Task] (Deadline: [Date])

## Open Issues
- [Issue 1]
- [Issue 2]

## Next Meeting
- Time: [Date and time]
- Topics: [Main discussion points]
```

## Extraction Principles

- **Focus on decisions**: Highlight important decisions made
- **Extract action items**: Capture all assigned tasks with owners and deadlines
- **Document issues**: Record problems discussed and their solutions
- **Remain objective**: Base summary only on meeting content, no speculation

## Content Rules

- **Concise**: Use brief language, avoid redundancy
- **Complete**: Ensure no key points or conclusions are missed
- **Accurate**: Reflect actual meeting content without distortion
- **Actionable**: Action items must be specific and executable

## Special Cases

- **Unclear info**: Mark as "[需确认]" rather than guessing
- **Multilingual**: Preserve original language; translate only when necessary
- **Technical terms**: Keep professional terms accurate; explain if needed
- **Sensitive info**: Apply appropriate masking for private or confidential content
