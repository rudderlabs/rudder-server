import * as React from 'react';

import FormGroup from '../../common/formGroup';
import formTemplatesMap from './destinationSettings.json';
import { Container } from './styles';

export interface IDestinationSettingsProps {
  destName: string;
  onSettingsChange: any;
  setRequirementsState: any;
  disabled?: boolean;
  initialSettings?: any;
}

export default class DestinationSettings extends React.Component<
  IDestinationSettingsProps,
  any
> {
  constructor(props: IDestinationSettingsProps) {
    super(props);
    this.state = {
      settings: {},
      reuqiresmentMet: true,
      formTemplate: (formTemplatesMap as { [key: string]: any })[
        props.destName
      ],
    };
  }

  public onSettingsChange = (settings: any) => {
    const { onSettingsChange, destName, setRequirementsState } = this.props;
    const { formTemplate } = this.state;
    onSettingsChange(settings);
    const req = Object.entries(settings).map(([k, v]) => {
      const fields = formTemplate.reduce(
        (acc: any, curr: any) => acc.concat(curr.fields),
        [],
      );
      if (!v) {
        const field = fields.find((field: any) => field.value === k);
        if (field.required) {
          return false;
        }
      }
      return true;
    });
    const reqmtsMet = req.reduce((acc: any, curr: any) => acc && curr, true);
    setRequirementsState(reqmtsMet);
  };

  public onChange = (settings: any) => {
    this.setState(
      (prevState: any) => ({
        settings: {
          ...prevState.settings,
          ...settings,
        },
      }),
      () => this.onSettingsChange(this.state.settings),
    );
  };

  public render() {
    const { destName, initialSettings, disabled } = this.props;
    const { formTemplate } = this.state;
    return (
      <Container className="p-t-lg p-b-lg">
        {formTemplate.map((group: any) => (
          <FormGroup
            key={group.title}
            title={group.title}
            fields={group.fields}
            onStateChange={this.onChange}
            initialSettings={initialSettings}
            disabled={!!disabled}
          ></FormGroup>
        ))}
      </Container>
    );
  }
}
