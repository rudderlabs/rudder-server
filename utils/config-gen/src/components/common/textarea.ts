import styled from 'styled-components';
import TextArea from 'antd/lib/input/TextArea';

export const Textarea = styled(TextArea)`
  height: 48px;
  background: ${({ theme }) => theme.color.grey50};
  border-radius: 2px;
  border: none;
  padding: 15px;

  font-size: ${({ theme }) => theme.fontSize.md};
  color: ${({ theme }) => theme.color.grey400};
  ::placeholder {
    /* Chrome, Firefox, Opera, Safari 10.1+ */
    color: ${({ theme }) => theme.color.grey200};
    opacity: 1; /* Firefox */
    line-height: 19px;
  }
  &:focus {
    border: 1px solid ${({ theme }) => theme.color.grey100};
    box-sizing: border-box;
    line-height: 22px;
    outline: none;
  }
`;
